(function () {
  var configEl = document.getElementById('meetConfig');
  var roomId = configEl ? configEl.getAttribute('data-room-id') : null;
  var userRole = configEl ? configEl.getAttribute('data-user-role') : '';
  var currentUserId = configEl ? configEl.getAttribute('data-user-id') : '';
  var statusEl = document.getElementById('meetStatus');
  var localVideo = document.getElementById('localVideo');
  var remoteVideo = document.getElementById('remoteVideo');
  var btnMic = document.getElementById('btnToggleMic');
  var btnCam = document.getElementById('btnToggleCam');
  var btnShare = document.getElementById('btnShareScreen');
  var btnEndMeeting = document.getElementById('btnEndMeeting');

  function setStatus(text) {
    if (statusEl) statusEl.textContent = text;
  }

  if (!roomId) {
    setStatus('غرفة غير صحيحة');
    return;
  }

  var pc = null;
  var localStream = null;
  var screenStream = null;
  var isMicEnabled = true;
  var isCamEnabled = true;
  var pollTimer = null;
  var pollDelayMs = 1000;
  var isDisposed = false;
  var hasSentJoin = false;
  var isCreatingOffer = false;
  var lastSignalSeq = 0;
  var pendingCandidates = [];

  var recorder = null;
  var recordedChunks = [];
  var isUploading = false;
  var shouldAutoRecord = (userRole === 'admin');

  var RTC_CONFIG = {
    iceServers: [
      { urls: 'stun:stun.l.google.com:19302' },
      { urls: 'stun:stun1.l.google.com:19302' }
    ]
  };

  function compareIds(a, b) {
    return String(a || '').localeCompare(String(b || ''));
  }

  function shouldInitiateOffer(signalEvent) {
    if (!signalEvent || !signalEvent.senderId || signalEvent.senderId === currentUserId) return false;
    if (userRole === 'admin' && signalEvent.senderRole !== 'admin') return true;
    if (userRole !== 'admin' && signalEvent.senderRole === 'admin') return false;
    return compareIds(currentUserId, signalEvent.senderId) < 0;
  }

  function scheduleSignalPoll(delay) {
    if (isDisposed) return;
    if (pollTimer) clearTimeout(pollTimer);
    pollTimer = setTimeout(fetchSignals, typeof delay === 'number' ? delay : pollDelayMs);
  }

  async function sendSignal(type, payload) {
    if (isDisposed) return;
    var resp = await fetch('/meet/' + encodeURIComponent(roomId) + '/signals', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      body: JSON.stringify({
        type: type,
        payload: payload || {}
      })
    });

    if (!resp.ok) {
      throw new Error('signal_post_failed_' + resp.status);
    }

    return resp.json().catch(function () { return { ok: true }; });
  }

  async function flushPendingCandidates() {
    if (!pc || !pc.remoteDescription) return;
    while (pendingCandidates.length) {
      var candidate = pendingCandidates.shift();
      try {
        await pc.addIceCandidate(new RTCIceCandidate(candidate));
      } catch (e) {}
    }
  }

  function ensurePeerConnection() {
    if (pc) return pc;

    pc = new RTCPeerConnection(RTC_CONFIG);

    pc.onicecandidate = function (event) {
      if (event.candidate) {
        sendSignal('ice-candidate', { candidate: event.candidate }).catch(function () {});
      }
    };

    pc.ontrack = function (event) {
      if (!remoteVideo) return;
      var stream = event.streams && event.streams[0] ? event.streams[0] : null;
      if (stream) remoteVideo.srcObject = stream;
      startAutoRecordingIfPossible();
    };

    pc.onconnectionstatechange = function () {
      setStatus('حالة الاتصال: ' + pc.connectionState);
      if (pc.connectionState === 'failed') {
        setStatus('فشل الاتصال المباشر. غالباً تحتاج TURN server للاجتماعات الأونلاين.');
      }
    };

    if (localStream) {
      localStream.getTracks().forEach(function (track) {
        pc.addTrack(track, localStream);
      });
    }

    return pc;
  }

  function updateButtons() {
    if (btnMic) btnMic.textContent = isMicEnabled ? 'الميك: شغال' : 'الميك: مقفول';
    if (btnCam) btnCam.textContent = isCamEnabled ? 'الكاميرا: شغالة' : 'الكاميرا: مقفولة';
  }

  function toggleMic() {
    if (!localStream) return;
    isMicEnabled = !isMicEnabled;
    localStream.getAudioTracks().forEach(function (track) { track.enabled = isMicEnabled; });
    updateButtons();
  }

  function toggleCam() {
    if (!localStream) return;
    isCamEnabled = !isCamEnabled;
    localStream.getVideoTracks().forEach(function (track) { track.enabled = isCamEnabled; });
    updateButtons();
  }

  function getVideoSender() {
    if (!pc) return null;
    var senders = pc.getSenders ? pc.getSenders() : [];
    for (var i = 0; i < senders.length; i += 1) {
      var sender = senders[i];
      if (sender && sender.track && sender.track.kind === 'video') return sender;
    }
    return null;
  }

  function stopScreenShare() {
    if (!screenStream) return;
    try {
      screenStream.getTracks().forEach(function (track) { track.stop(); });
    } catch (e) {}
    screenStream = null;

    var sender = getVideoSender();
    var camTrack = localStream && localStream.getVideoTracks && localStream.getVideoTracks()[0];
    if (sender && camTrack && sender.replaceTrack) {
      sender.replaceTrack(camTrack);
    }
    if (localVideo && localStream) localVideo.srcObject = localStream;
    if (btnShare) btnShare.textContent = 'مشاركة الشاشة';
  }

  async function shareScreen() {
    if (screenStream) {
      stopScreenShare();
      return;
    }

    if (!navigator.mediaDevices || !navigator.mediaDevices.getDisplayMedia) {
      setStatus('مشاركة الشاشة غير مدعومة على هذا المتصفح');
      return;
    }

    try {
      screenStream = await navigator.mediaDevices.getDisplayMedia({ video: true, audio: false });
      var screenTrack = screenStream.getVideoTracks()[0];
      if (!screenTrack) return;

      ensurePeerConnection();
      var sender = getVideoSender();
      if (sender && sender.replaceTrack) {
        sender.replaceTrack(screenTrack);
      }

      if (localVideo) localVideo.srcObject = screenStream;
      if (btnShare) btnShare.textContent = 'إيقاف مشاركة الشاشة';

      screenTrack.addEventListener('ended', function () {
        stopScreenShare();
      });
    } catch (e) {
      setStatus('فشل مشاركة الشاشة');
    }
  }

  async function startLocalMedia() {
    if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia) {
      setStatus('المتصفح لا يدعم تشغيل الكاميرا/الميك');
      return;
    }

    setStatus('جاري تشغيل الكاميرا والميك...');
    try {
      localStream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
      if (localVideo) localVideo.srcObject = localStream;
      updateButtons();
      setStatus('تم تشغيل الكاميرا والميك. في انتظار الطرف الآخر...');
    } catch (e) {
      setStatus('تعذر تشغيل الكاميرا/الميك. تأكد من الصلاحيات');
      throw e;
    }
  }

  async function createOfferAndSend() {
    if (isCreatingOffer) return;
    var peer = ensurePeerConnection();
    if (peer.signalingState !== 'stable') return;

    isCreatingOffer = true;
    setStatus('جاري إنشاء الاتصال...');

    try {
      var offer = await peer.createOffer();
      await peer.setLocalDescription(offer);
      await sendSignal('offer', { offer: offer });
    } finally {
      isCreatingOffer = false;
    }
  }

  async function handleOffer(offer) {
    var peer = ensurePeerConnection();
    if (peer.signalingState !== 'stable') return;
    await peer.setRemoteDescription(new RTCSessionDescription(offer));
    await flushPendingCandidates();
    var answer = await peer.createAnswer();
    await peer.setLocalDescription(answer);
    await sendSignal('answer', { answer: answer });
  }

  async function handleAnswer(answer) {
    if (!pc) return;
    await pc.setRemoteDescription(new RTCSessionDescription(answer));
    await flushPendingCandidates();
  }

  async function handleCandidate(candidate) {
    if (!candidate) return;
    if (!pc || !pc.remoteDescription) {
      pendingCandidates.push(candidate);
      return;
    }

    try {
      await pc.addIceCandidate(new RTCIceCandidate(candidate));
    } catch (e) {}
  }

  async function fetchSignals() {
    if (isDisposed) return;

    try {
      var resp = await fetch('/meet/' + encodeURIComponent(roomId) + '/signals?after=' + encodeURIComponent(lastSignalSeq), {
        headers: { 'Accept': 'application/json' },
        cache: 'no-store'
      });

      if (!resp.ok) {
        setStatus('تعذر مزامنة الاجتماع مع السيرفر');
        scheduleSignalPoll(2000);
        return;
      }

      var data = await resp.json();
      var events = Array.isArray(data && data.events) ? data.events : [];
      for (var i = 0; i < events.length; i += 1) {
        var event = events[i];
        lastSignalSeq = Math.max(lastSignalSeq, Number(event && event.seq) || 0);
        await handleSignalEvent(event);
      }
    } catch (e) {
      setStatus('تعذر الوصول إلى إشارات الاجتماع. جاري إعادة المحاولة...');
    }

    scheduleSignalPoll();
  }

  async function handleSignalEvent(event) {
    if (!event || !event.type) return;

    if (event.type === 'join') {
      setStatus('دخل الطرف الآخر إلى الغرفة');
      if (shouldInitiateOffer(event)) {
        try {
          await createOfferAndSend();
        } catch (e) {
          setStatus('حدث خطأ أثناء إنشاء الاتصال');
        }
      }
      startAutoRecordingIfPossible();
      return;
    }

    if (event.type === 'leave') {
      setStatus('الطرف الآخر خرج من الاجتماع');
      if (remoteVideo) remoteVideo.srcObject = null;
      stopAndUploadRecording();
      return;
    }

    if (event.type === 'offer' && event.payload && event.payload.offer) {
      try {
        await handleOffer(event.payload.offer);
      } catch (e) {
        setStatus('فشل استقبال الاتصال');
      }
      return;
    }

    if (event.type === 'answer' && event.payload && event.payload.answer) {
      try {
        await handleAnswer(event.payload.answer);
      } catch (e) {
        setStatus('فشل تثبيت الاتصال');
      }
      return;
    }

    if (event.type === 'ice-candidate' && event.payload && event.payload.candidate) {
      await handleCandidate(event.payload.candidate);
    }
  }

  function buildRecordingStream() {
    var tracks = [];
    var remoteStream = remoteVideo && remoteVideo.srcObject ? remoteVideo.srcObject : null;
    var local = localStream;

    if (remoteStream && remoteStream.getVideoTracks && remoteStream.getVideoTracks().length) {
      tracks.push(remoteStream.getVideoTracks()[0]);
    } else if (local && local.getVideoTracks && local.getVideoTracks().length) {
      tracks.push(local.getVideoTracks()[0]);
    }

    if (local && local.getAudioTracks) {
      local.getAudioTracks().forEach(function (track) { tracks.push(track); });
    }
    if (remoteStream && remoteStream.getAudioTracks) {
      remoteStream.getAudioTracks().forEach(function (track) { tracks.push(track); });
    }

    return new MediaStream(tracks);
  }

  function getSupportedMimeType() {
    var candidates = [
      'video/webm;codecs=vp9,opus',
      'video/webm;codecs=vp8,opus',
      'video/webm;codecs=vp8',
      'video/webm'
    ];
    for (var i = 0; i < candidates.length; i += 1) {
      var mimeType = candidates[i];
      if (window.MediaRecorder && MediaRecorder.isTypeSupported && MediaRecorder.isTypeSupported(mimeType)) {
        return mimeType;
      }
    }
    return '';
  }

  function startAutoRecordingIfPossible() {
    if (!shouldAutoRecord) return;
    if (recorder) return;
    if (!window.MediaRecorder) {
      setStatus('المتصفح لا يدعم تسجيل الاجتماع');
      return;
    }

    var remoteStream = remoteVideo && remoteVideo.srcObject ? remoteVideo.srcObject : null;
    if (!localStream || !remoteStream) {
      setStatus('في انتظار الطرف الآخر...');
      return;
    }

    try {
      recordedChunks = [];
      var stream = buildRecordingStream();
      var mimeType = getSupportedMimeType();
      var options = mimeType ? { mimeType: mimeType } : undefined;
      recorder = new MediaRecorder(stream, options);

      recorder.ondataavailable = function (event) {
        if (event.data && event.data.size > 0) recordedChunks.push(event.data);
      };

      recorder.onstart = function () {
        setStatus('جاري التسجيل تلقائياً...');
      };

      recorder.start(1000);
    } catch (e) {
      setStatus('فشل بدء التسجيل');
      recorder = null;
    }
  }

  async function stopAndUploadRecording() {
    if (!shouldAutoRecord) return;
    if (isUploading) return;

    if (recorder && recorder.state !== 'inactive') {
      try { recorder.stop(); } catch (e) {}
    }

    await new Promise(function (resolve) { setTimeout(resolve, 250); });

    if (!recordedChunks.length) {
      recorder = null;
      return;
    }

    isUploading = true;
    setStatus('جاري رفع التسجيل...');

    try {
      var mimeType = recorder && recorder.mimeType ? recorder.mimeType : 'video/webm';
      var blob = new Blob(recordedChunks, { type: mimeType || 'video/webm' });
      var form = new FormData();
      form.append('recording', blob, 'meeting.webm');

      var resp = await fetch('/meet/' + encodeURIComponent(roomId) + '/recording', {
        method: 'POST',
        body: form
      });

      if (!resp.ok) {
        setStatus('فشل رفع التسجيل');
      } else {
        setStatus('تم حفظ التسجيل');
      }
    } catch (e) {
      setStatus('فشل رفع التسجيل');
    } finally {
      isUploading = false;
      recorder = null;
      recordedChunks = [];
    }
  }

  async function announceJoin() {
    if (hasSentJoin) return;
    hasSentJoin = true;
    try {
      await sendSignal('join', { role: userRole });
    } catch (e) {
      setStatus('تعذر إشعار الطرف الآخر بدخولك إلى الاجتماع');
    }
  }

  async function leaveMeeting() {
    if (isDisposed) return;
    if (pollTimer) clearTimeout(pollTimer);
    try { await sendSignal('leave', {}); } catch (e) {}
    isDisposed = true;
    try { if (pc) pc.close(); } catch (e2) {}
    try { if (localStream) localStream.getTracks().forEach(function (track) { track.stop(); }); } catch (e3) {}
    try { if (screenStream) screenStream.getTracks().forEach(function (track) { track.stop(); }); } catch (e4) {}
  }

  async function init() {
    await startLocalMedia();
    ensurePeerConnection();
    scheduleSignalPoll(0);
    await announceJoin();

    window.addEventListener('beforeunload', function () {
      try { navigator.sendBeacon('/meet/' + encodeURIComponent(roomId) + '/signals', new Blob([JSON.stringify({ type: 'leave', payload: {} })], { type: 'application/json' })); } catch (e) {}
      try { leaveMeeting(); } catch (e2) {}
      try { stopAndUploadRecording(); } catch (e3) {}
    });

    if (btnMic) btnMic.addEventListener('click', toggleMic);
    if (btnCam) btnCam.addEventListener('click', toggleCam);
    if (btnShare) btnShare.addEventListener('click', function () {
      shareScreen();
    });
    if (btnEndMeeting) btnEndMeeting.addEventListener('click', async function () {
      await stopAndUploadRecording();
      await leaveMeeting();
      setTimeout(function () {
        window.location.href = userRole === 'admin' ? '/admin/appointments' : '/my-appointments';
      }, 500);
    });
  }

  init().catch(function () {
    // status already handled where possible
  });
})();
