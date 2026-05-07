(function () {
  var configEl = document.getElementById('meetConfig');
  var roomId = configEl ? configEl.getAttribute('data-room-id') : null;
  var returnUrl = configEl ? (configEl.getAttribute('data-return-url') || '/my-appointments') : '/my-appointments';
  var liveKitEnabled = configEl ? configEl.getAttribute('data-livekit-enabled') === '1' : false;
  var userRole = configEl ? (configEl.getAttribute('data-user-role') || '') : '';
  var statusEl = document.getElementById('meetStatus');
  var localVideo = document.getElementById('localVideo');
  var remoteParticipantsEl = document.getElementById('remoteParticipants');
  var remoteEmptyStateEl = document.getElementById('remoteEmptyState');
  var btnMic = document.getElementById('btnToggleMic');
  var btnCam = document.getElementById('btnToggleCam');
  var btnShare = document.getElementById('btnShareScreen');
  var btnEndMeeting = document.getElementById('btnEndMeeting');

  var room = null;
  var isMicEnabled = true;
  var isCamEnabled = true;
  var isScreenEnabled = false;
  var remoteTrackMap = new Map();
  var isAdminRecorder = userRole === 'admin' || userRole === 'superadmin';
  var meetingRecorder = null;
  var recordingChunks = [];
  var recordingCanvas = null;
  var recordingContext = null;
  var recordingAnimationFrame = null;
  var recordingAudioContext = null;
  var recordingAudioDestination = null;
  var audioTrackSourceMap = new Map();
  var hasUploadedRecording = false;
  var recordingStartedAt = null;
  var isUploadingRecording = false;

  function setStatus(text) {
    if (statusEl) statusEl.textContent = text;
  }

  function setRecordingStatus(text) {
    if (!text) return;
    setStatus(text);
  }

  function ensureLiveKit() {
    return window.LivekitClient || window.livekit;
  }

  function getRemoteContainer(participantSid, participantName) {
    if (!remoteParticipantsEl) return null;

    var existing = remoteParticipantsEl.querySelector('[data-participant-sid="' + participantSid + '"]');
    if (existing) return existing;

    var wrapper = document.createElement('div');
    wrapper.className = 'meet-remote-tile';
    wrapper.setAttribute('data-participant-sid', participantSid);

    var videoWrap = document.createElement('div');
    videoWrap.className = 'meet-remote-video';

    var label = document.createElement('div');
    label.className = 'meet-remote-label';
    label.textContent = participantName || 'مشارك';

    wrapper.appendChild(videoWrap);
    wrapper.appendChild(label);
    remoteParticipantsEl.appendChild(wrapper);
    syncRemoteState();
    return wrapper;
  }

  function syncRemoteState() {
    if (!remoteParticipantsEl || !remoteEmptyStateEl) return;
    var tiles = remoteParticipantsEl.querySelectorAll('.meet-remote-tile');
    remoteEmptyStateEl.style.display = tiles.length ? 'none' : 'flex';
  }

  function removeRemoteContainer(participantSid) {
    if (!remoteParticipantsEl) return;
    var existing = remoteParticipantsEl.querySelector('[data-participant-sid="' + participantSid + '"]');
    if (existing) existing.remove();
    remoteTrackMap.delete(participantSid);
    syncRemoteState();
  }

  function attachLocalTracks() {
    if (!room || !localVideo) return;
    var publications = Array.from(room.localParticipant.videoTrackPublications.values());
    var cameraPub = publications.find(function (pub) {
      return pub && pub.source === 'camera' && pub.track;
    });

    if (!cameraPub || !cameraPub.track) return;

    try {
      localVideo.srcObject = null;
      localVideo.innerHTML = '';
      var attached = cameraPub.track.attach();
      attached.muted = true;
      attached.autoplay = true;
      attached.playsInline = true;
      localVideo.replaceWith(attached);
      attached.id = 'localVideo';
      localVideo = attached;
    } catch (e) {}
  }

  function attachRemoteTrack(participant, track) {
    if (!participant || !track || !remoteParticipantsEl) return;
    var container = getRemoteContainer(participant.sid, participant.name);
    if (!container) return;

    var videoWrap = container.querySelector('.meet-remote-video');
    if (!videoWrap) return;

    if (track.kind === 'video') {
      var old = remoteTrackMap.get(participant.sid);
      if (old && old !== track) {
        try { old.detach().forEach(function (el) { el.remove(); }); } catch (e) {}
      }
      videoWrap.innerHTML = '';
      var el = track.attach();
      el.autoplay = true;
      el.playsInline = true;
      videoWrap.appendChild(el);
      remoteTrackMap.set(participant.sid, track);
    }

    if (track.kind === 'audio') {
      var audioEl = track.attach();
      audioEl.autoplay = true;
      videoWrap.appendChild(audioEl);
    }
  }

  function attachExistingParticipantTracks(participant) {
    if (!participant) return;
    participant.trackPublications.forEach(function (publication) {
      if (publication && publication.isSubscribed && publication.track) {
        attachRemoteTrack(participant, publication.track);
      }
    });
  }

  function getRecordableMediaElements() {
    var mediaEls = [];
    if (localVideo && localVideo.tagName === 'VIDEO') {
      mediaEls.push({ element: localVideo, label: 'أنت' });
    }
    if (remoteParticipantsEl) {
      remoteParticipantsEl.querySelectorAll('.meet-remote-tile').forEach(function (tile) {
        var labelEl = tile.querySelector('.meet-remote-label');
        var videoEl = tile.querySelector('video');
        if (videoEl) {
          mediaEls.push({ element: videoEl, label: labelEl ? labelEl.textContent : 'مشارك' });
        }
      });
    }
    return mediaEls;
  }

  function drawRecordingFrame() {
    if (!recordingContext || !recordingCanvas) return;

    var width = recordingCanvas.width;
    var height = recordingCanvas.height;
    recordingContext.clearRect(0, 0, width, height);

    var gradient = recordingContext.createLinearGradient(0, 0, width, height);
    gradient.addColorStop(0, '#090b12');
    gradient.addColorStop(1, '#191d2d');
    recordingContext.fillStyle = gradient;
    recordingContext.fillRect(0, 0, width, height);

    var entries = getRecordableMediaElements();
    var cards = entries.length ? entries : [];
    var cols = cards.length > 1 ? 2 : 1;
    var rows = Math.max(1, Math.ceil(cards.length / cols));
    var gap = 24;
    var padding = 28;
    var cardWidth = (width - padding * 2 - gap * (cols - 1)) / cols;
    var cardHeight = (height - padding * 2 - gap * (rows - 1)) / rows;

    if (!cards.length) {
      recordingContext.fillStyle = 'rgba(255,255,255,0.9)';
      recordingContext.font = '600 34px system-ui';
      recordingContext.textAlign = 'center';
      recordingContext.fillText('في انتظار المشاركين...', width / 2, height / 2);
    }

    cards.forEach(function (entry, index) {
      var col = index % cols;
      var row = Math.floor(index / cols);
      var x = padding + col * (cardWidth + gap);
      var y = padding + row * (cardHeight + gap);
      var label = entry.label || 'مشارك';

      recordingContext.fillStyle = 'rgba(255,255,255,0.06)';
      recordingContext.strokeStyle = 'rgba(255,255,255,0.18)';
      recordingContext.lineWidth = 2;
      recordingContext.beginPath();
      recordingContext.roundRect(x, y, cardWidth, cardHeight, 26);
      recordingContext.fill();
      recordingContext.stroke();

      try {
        if (entry.element.readyState >= 2) {
          recordingContext.save();
          recordingContext.beginPath();
          recordingContext.roundRect(x + 4, y + 4, cardWidth - 8, cardHeight - 8, 22);
          recordingContext.clip();
          recordingContext.drawImage(entry.element, x + 4, y + 4, cardWidth - 8, cardHeight - 8);
          recordingContext.restore();
        }
      } catch (error) {}

      recordingContext.fillStyle = 'rgba(9, 11, 18, 0.72)';
      recordingContext.beginPath();
      recordingContext.roundRect(x + 18, y + cardHeight - 72, Math.min(cardWidth - 36, 220), 42, 18);
      recordingContext.fill();

      recordingContext.fillStyle = '#ffffff';
      recordingContext.font = '600 24px system-ui';
      recordingContext.textAlign = 'right';
      recordingContext.fillText(label, x + cardWidth - 28, y + cardHeight - 42);
    });

    recordingContext.fillStyle = 'rgba(255,255,255,0.9)';
    recordingContext.font = '600 22px system-ui';
    recordingContext.textAlign = 'left';
    recordingContext.fillText('Codentra Meeting', 28, 38);

    recordingContext.fillStyle = '#ff5f57';
    recordingContext.beginPath();
    recordingContext.arc(width - 40, 34, 10, 0, Math.PI * 2);
    recordingContext.fill();
    recordingContext.fillStyle = 'rgba(255,255,255,0.85)';
    recordingContext.font = '600 18px system-ui';
    recordingContext.textAlign = 'right';
    recordingContext.fillText('جارٍ التسجيل', width - 60, 40);

    recordingAnimationFrame = window.requestAnimationFrame(drawRecordingFrame);
  }

  function connectAudioTrack(track, key) {
    if (!recordingAudioContext || !recordingAudioDestination || !track || !key || audioTrackSourceMap.has(key)) return;
    var mediaStreamTrack = track.mediaStreamTrack || track._mediaStreamTrack || null;
    if (!mediaStreamTrack || mediaStreamTrack.kind !== 'audio') return;
    try {
      var stream = new MediaStream([mediaStreamTrack]);
      var source = recordingAudioContext.createMediaStreamSource(stream);
      source.connect(recordingAudioDestination);
      audioTrackSourceMap.set(key, { source: source, stream: stream });
    } catch (error) {
      console.error('Audio track wiring failed', error);
    }
  }

  function refreshRecordingAudioInputs() {
    if (!recordingAudioContext || !recordingAudioDestination || !room) return;

    room.localParticipant.audioTrackPublications.forEach(function (publication) {
      if (publication && publication.track) {
        connectAudioTrack(publication.track, 'local:' + publication.trackSid);
      }
    });

    room.remoteParticipants.forEach(function (participant) {
      participant.audioTrackPublications.forEach(function (publication) {
        if (publication && publication.track) {
          connectAudioTrack(publication.track, participant.sid + ':' + publication.trackSid);
        }
      });
    });
  }

  async function startAutomaticRecording() {
    if (!isAdminRecorder || meetingRecorder || !window.MediaRecorder) return;

    try {
      recordingCanvas = document.createElement('canvas');
      recordingCanvas.width = 1280;
      recordingCanvas.height = 720;
      recordingContext = recordingCanvas.getContext('2d');
      recordingAudioContext = new (window.AudioContext || window.webkitAudioContext)();
      if (recordingAudioContext.state === 'suspended') {
        await recordingAudioContext.resume().catch(function () {});
      }
      recordingAudioDestination = recordingAudioContext.createMediaStreamDestination();
      refreshRecordingAudioInputs();
      drawRecordingFrame();

      var videoStream = recordingCanvas.captureStream(24);
      var tracks = [];
      videoStream.getVideoTracks().forEach(function (track) { tracks.push(track); });
      recordingAudioDestination.stream.getAudioTracks().forEach(function (track) { tracks.push(track); });
      var finalStream = new MediaStream(tracks);

      var options = [
        'video/webm;codecs=vp9,opus',
        'video/webm;codecs=vp8,opus',
        'video/webm'
      ].find(function (mimeType) {
        return window.MediaRecorder.isTypeSupported(mimeType);
      });

      meetingRecorder = new MediaRecorder(finalStream, options ? { mimeType: options } : undefined);
      recordingChunks = [];
      recordingStartedAt = new Date().toISOString();
      hasUploadedRecording = false;

      meetingRecorder.ondataavailable = function (event) {
        if (event.data && event.data.size) recordingChunks.push(event.data);
      };

      meetingRecorder.start(2000);
      setRecordingStatus('تم بدء التسجيل التلقائي وسيُرفع بعد إنهاء الاجتماع');
    } catch (error) {
      console.error('Auto recording failed to start', error);
      setRecordingStatus('تعذر بدء التسجيل التلقائي');
    }
  }

  async function uploadMeetingRecording(blob) {
    if (!blob || !blob.size || hasUploadedRecording || isUploadingRecording) return;
    isUploadingRecording = true;

    try {
      var formData = new FormData();
      var stamp = new Date().toISOString().replace(/[:.]/g, '-');
      formData.append('recording', blob, 'meeting-' + roomId + '-' + stamp + '.webm');
      if (recordingStartedAt) formData.append('recordingStartedAt', recordingStartedAt);

      var response = await fetch('/meet/' + encodeURIComponent(roomId) + '/recording', {
        method: 'POST',
        body: formData
      });
      var data = await response.json().catch(function () { return {}; });
      if (!response.ok || !data.ok) {
        throw new Error(data.error || 'فشل رفع التسجيل');
      }
      hasUploadedRecording = true;
      setRecordingStatus('تم حفظ تسجيل الاجتماع ورفعه بنجاح');
    } catch (error) {
      console.error('Recording upload failed', error);
      setRecordingStatus('انتهى الاجتماع لكن رفع التسجيل لم يكتمل');
    } finally {
      isUploadingRecording = false;
    }
  }

  async function stopAutomaticRecording() {
    if (!meetingRecorder) return;

    var recorder = meetingRecorder;
    meetingRecorder = null;

    var blob = await new Promise(function (resolve) {
      recorder.onstop = function () {
        resolve(recordingChunks.length ? new Blob(recordingChunks, { type: recorder.mimeType || 'video/webm' }) : null);
      };
      try {
        if (recorder.state !== 'inactive') recorder.stop();
        else resolve(null);
      } catch (error) {
        resolve(null);
      }
    });

    try {
      if (recordingAnimationFrame) window.cancelAnimationFrame(recordingAnimationFrame);
      recordingAnimationFrame = null;
      if (recordingAudioContext) recordingAudioContext.close().catch(function () {});
    } catch (error) {}

    [recordingCanvas, recordingContext, recordingAudioContext, recordingAudioDestination].forEach(function () {});
    recordingCanvas = null;
    recordingContext = null;
    recordingAudioContext = null;
    recordingAudioDestination = null;
    audioTrackSourceMap.forEach(function (entry) {
      try { entry.source.disconnect(); } catch (error) {}
    });
    audioTrackSourceMap = new Map();

    await uploadMeetingRecording(blob);
    recordingChunks = [];
  }

  async function fetchMeetingToken() {
    var resp = await fetch('/meet/' + encodeURIComponent(roomId) + '/token', {
      headers: { Accept: 'application/json' }
    });
    var data = await resp.json().catch(function () { return {}; });
    if (!resp.ok || !data.ok) {
      throw new Error(data.error || 'تعذر بدء الاجتماع');
    }
    return data;
  }

  async function connectRoom() {
    if (!roomId) {
      setStatus('غرفة غير صحيحة');
      return;
    }

    if (!liveKitEnabled) {
      setStatus('LiveKit غير مُفعّل بعد. أضف LIVEKIT_URL وLIVEKIT_API_KEY وLIVEKIT_API_SECRET.');
      return;
    }

    var LivekitClient = ensureLiveKit();
    if (!LivekitClient) {
      setStatus('تعذر تحميل مكتبة LiveKit');
      return;
    }

    setStatus('جاري تجهيز الاجتماع...');
    var credentials = await fetchMeetingToken();

    room = new LivekitClient.Room({
      adaptiveStream: true,
      dynacast: true,
      videoCaptureDefaults: {
        resolution: LivekitClient.VideoPresets.h720.resolution
      }
    });

    room
      .on(LivekitClient.RoomEvent.TrackSubscribed, function (track, publication, participant) {
        attachRemoteTrack(participant, track);
        refreshRecordingAudioInputs();
        setStatus('تم اتصال المشاركين بنجاح');
      })
      .on(LivekitClient.RoomEvent.TrackUnsubscribed, function (track, publication, participant) {
        try { track.detach().forEach(function (el) { el.remove(); }); } catch (e) {}
        if (track.kind === 'video') removeRemoteContainer(participant.sid);
      })
      .on(LivekitClient.RoomEvent.ParticipantDisconnected, function (participant) {
        removeRemoteContainer(participant.sid);
        setStatus('غادر أحد المشاركين الغرفة');
      })
      .on(LivekitClient.RoomEvent.ParticipantConnected, function (participant) {
        getRemoteContainer(participant.sid, participant.name);
        setStatus('انضم مشارك جديد إلى الاجتماع');
      })
      .on(LivekitClient.RoomEvent.LocalTrackPublished, function () {
        attachLocalTracks();
        refreshRecordingAudioInputs();
      })
      .on(LivekitClient.RoomEvent.ConnectionQualityChanged, function () {
        setStatus('الاتصال مستقر عبر LiveKit');
      })
      .on(LivekitClient.RoomEvent.Disconnected, function () {
        setStatus('تم إنهاء الاتصال بالغرفة');
      });

    await room.connect(credentials.url, credentials.token);
    setStatus('تم الدخول إلى الاجتماع');

    await room.localParticipant.setCameraEnabled(true);
    await room.localParticipant.setMicrophoneEnabled(true);
    attachLocalTracks();
    updateButtons();

    room.remoteParticipants.forEach(function (participant) {
      getRemoteContainer(participant.sid, participant.name);
      attachExistingParticipantTracks(participant);
    });
    syncRemoteState();
    refreshRecordingAudioInputs();
    await startAutomaticRecording();
  }

  function updateButtons() {
    if (btnMic) btnMic.textContent = isMicEnabled ? 'الميك: شغال' : 'الميك: مقفول';
    if (btnCam) btnCam.textContent = isCamEnabled ? 'الكاميرا: شغالة' : 'الكاميرا: مقفولة';
    if (btnShare) btnShare.textContent = isScreenEnabled ? 'إيقاف مشاركة الشاشة' : 'مشاركة الشاشة';
  }

  async function toggleMic() {
    if (!room) return;
    isMicEnabled = !isMicEnabled;
    await room.localParticipant.setMicrophoneEnabled(isMicEnabled);
    updateButtons();
  }

  async function toggleCam() {
    if (!room) return;
    isCamEnabled = !isCamEnabled;
    await room.localParticipant.setCameraEnabled(isCamEnabled);
    if (isCamEnabled) attachLocalTracks();
    updateButtons();
  }

  async function toggleScreenShare() {
    if (!room) return;
    try {
      isScreenEnabled = !isScreenEnabled;
      await room.localParticipant.setScreenShareEnabled(isScreenEnabled);
      updateButtons();
      setStatus(isScreenEnabled ? 'تم بدء مشاركة الشاشة' : 'تم إيقاف مشاركة الشاشة');
    } catch (error) {
      isScreenEnabled = false;
      updateButtons();
      setStatus('تعذر تشغيل مشاركة الشاشة');
    }
  }

  async function leaveMeeting() {
    btnEndMeeting && (btnEndMeeting.disabled = true);
    setStatus('جاري إنهاء الاجتماع...');
    try {
      await stopAutomaticRecording();
    } catch (error) {
      console.error(error);
    }
    try {
      if (room) room.disconnect();
    } catch (e) {}
    window.location.href = returnUrl;
  }

  if (btnMic) btnMic.addEventListener('click', function () { toggleMic().catch(function () {}); });
  if (btnCam) btnCam.addEventListener('click', function () { toggleCam().catch(function () {}); });
  if (btnShare) btnShare.addEventListener('click', function () { toggleScreenShare().catch(function () {}); });
  if (btnEndMeeting) btnEndMeeting.addEventListener('click', function () { leaveMeeting(); });

  window.addEventListener('beforeunload', function () {
    try {
      if (meetingRecorder && meetingRecorder.state !== 'inactive') meetingRecorder.stop();
      if (room) room.disconnect();
    } catch (e) {}
  });

  connectRoom().catch(function (error) {
    console.error(error);
    setStatus(error && error.message ? error.message : 'تعذر بدء الاجتماع');
  });
})();
