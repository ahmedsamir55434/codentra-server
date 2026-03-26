(function () {
  var configEl = document.getElementById('meetConfig');
  var roomId = configEl ? configEl.getAttribute('data-room-id') : null;
  var userRole = configEl ? configEl.getAttribute('data-user-role') : '';
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

  var socket = window.io ? window.io() : null;
  if (!socket) {
    setStatus('تعذر الاتصال بالسيرفر');
    return;
  }

  var pc = null;
  var localStream = null;
  var screenStream = null;
  var isMicEnabled = true;
  var isCamEnabled = true;

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

  function ensurePeerConnection() {
    if (pc) return pc;

    pc = new RTCPeerConnection(RTC_CONFIG);

    pc.onicecandidate = function (event) {
      if (event.candidate) {
        socket.emit('webrtc-ice-candidate', { roomId: roomId, candidate: event.candidate });
      }
    };

    pc.ontrack = function (event) {
      if (!remoteVideo) return;
      var stream = event.streams && event.streams[0] ? event.streams[0] : null;
      if (stream) remoteVideo.srcObject = stream;

      // Remote media is ready: start recording on admin side.
      startAutoRecordingIfPossible();
    };

    pc.onconnectionstatechange = function () {
      setStatus('حالة الاتصال: ' + pc.connectionState);
    };

    if (localStream) {
      localStream.getTracks().forEach(function (t) {
        pc.addTrack(t, localStream);
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
    localStream.getAudioTracks().forEach(function (t) { t.enabled = isMicEnabled; });
    updateButtons();
  }

  function toggleCam() {
    if (!localStream) return;
    isCamEnabled = !isCamEnabled;
    localStream.getVideoTracks().forEach(function (t) { t.enabled = isCamEnabled; });
    updateButtons();
  }

  function getVideoSender() {
    if (!pc) return null;
    var senders = pc.getSenders ? pc.getSenders() : [];
    for (var i = 0; i < senders.length; i++) {
      var s = senders[i];
      if (s && s.track && s.track.kind === 'video') return s;
    }
    return null;
  }

  function stopScreenShare() {
    if (!screenStream) return;
    try {
      screenStream.getTracks().forEach(function (t) { t.stop(); });
    } catch (e) {}
    screenStream = null;

    // Restore camera video track
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

      var sender = getVideoSender();
      ensurePeerConnection();
      sender = getVideoSender();

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
    var peer = ensurePeerConnection();
    setStatus('جاري إنشاء اتصال...');

    var offer = await peer.createOffer();
    await peer.setLocalDescription(offer);
    socket.emit('webrtc-offer', { roomId: roomId, offer: offer });
  }

  async function handleOffer(offer) {
    var peer = ensurePeerConnection();
    await peer.setRemoteDescription(new RTCSessionDescription(offer));
    var answer = await peer.createAnswer();
    await peer.setLocalDescription(answer);
    socket.emit('webrtc-answer', { roomId: roomId, answer: answer });
  }

  async function handleAnswer(answer) {
    if (!pc) return;
    await pc.setRemoteDescription(new RTCSessionDescription(answer));
  }

  async function handleCandidate(candidate) {
    if (!pc) return;
    try {
      await pc.addIceCandidate(new RTCIceCandidate(candidate));
    } catch (e) {}
  }

  function buildRecordingStream() {
    var tracks = [];

    var remoteStream = remoteVideo && remoteVideo.srcObject ? remoteVideo.srcObject : null;
    var local = localStream;

    // Prefer remote video if available (captures the other side), fallback to local camera.
    if (remoteStream && remoteStream.getVideoTracks && remoteStream.getVideoTracks().length) {
      tracks.push(remoteStream.getVideoTracks()[0]);
    } else if (local && local.getVideoTracks && local.getVideoTracks().length) {
      tracks.push(local.getVideoTracks()[0]);
    }

    // Add audio tracks from both sides if available.
    if (local && local.getAudioTracks) {
      local.getAudioTracks().forEach(function (t) { tracks.push(t); });
    }
    if (remoteStream && remoteStream.getAudioTracks) {
      remoteStream.getAudioTracks().forEach(function (t) { tracks.push(t); });
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
    for (var i = 0; i < candidates.length; i++) {
      var mt = candidates[i];
      if (window.MediaRecorder && MediaRecorder.isTypeSupported && MediaRecorder.isTypeSupported(mt)) return mt;
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
    // Ensure both local and remote streams are ready
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

      recorder.onstop = function () {
        // upload happens in stopAndUploadRecording
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

    // Give MediaRecorder a moment to flush final chunk.
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

  async function init() {
    await startLocalMedia();
    ensurePeerConnection();

    socket.emit('join-room', roomId);

    socket.on('peer-joined', function () {
      // If a new peer joined after us, we create an offer.
      createOfferAndSend().catch(function () {
        setStatus('حدث خطأ أثناء إنشاء الاتصال');
      });

      // Start recording automatically on admin side when peer joins.
      startAutoRecordingIfPossible();
    });

    socket.on('peer-left', function () {
      setStatus('الطرف الآخر خرج من الاجتماع');
      if (remoteVideo) remoteVideo.srcObject = null;

      // Stop and upload recording when peer leaves.
      stopAndUploadRecording();
    });

    socket.on('webrtc-offer', function (payload) {
      if (!payload || !payload.offer) return;
      handleOffer(payload.offer).catch(function () {
        setStatus('فشل استقبال الاتصال');
      });
    });

    socket.on('webrtc-answer', function (payload) {
      if (!payload || !payload.answer) return;
      handleAnswer(payload.answer).catch(function () {
        setStatus('فشل تثبيت الاتصال');
      });
    });

    socket.on('webrtc-ice-candidate', function (payload) {
      if (!payload || !payload.candidate) return;
      handleCandidate(payload.candidate);
    });

    window.addEventListener('beforeunload', function () {
      try { socket.emit('leave-room', roomId); } catch (e) {}
      try { if (pc) pc.close(); } catch (e2) {}
      try { if (localStream) localStream.getTracks().forEach(function (t) { t.stop(); }); } catch (e3) {}
      try { if (screenStream) screenStream.getTracks().forEach(function (t) { t.stop(); }); } catch (e4) {}

      // Attempt to stop and upload. This might not always finish before unload.
      try { stopAndUploadRecording(); } catch (e5) {}
    });

    if (btnMic) btnMic.addEventListener('click', toggleMic);
    if (btnCam) btnCam.addEventListener('click', toggleCam);
    if (btnShare) btnShare.addEventListener('click', function () {
      shareScreen();
    });
    if (btnEndMeeting) btnEndMeeting.addEventListener('click', async function () {
      await stopAndUploadRecording();
      // Give a moment for upload to start, then redirect
      setTimeout(function () {
        window.location.href = '/my-appointments';
      }, 500);
    });
  }

  init().catch(function () {
    // handled above
  });
})();
