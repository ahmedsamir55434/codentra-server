(function () {
  var configEl = document.getElementById('meetConfig');
  var roomId = configEl ? configEl.getAttribute('data-room-id') : null;
  var returnUrl = configEl ? (configEl.getAttribute('data-return-url') || '/my-appointments') : '/my-appointments';
  var liveKitEnabled = configEl ? configEl.getAttribute('data-livekit-enabled') === '1' : false;
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

  function setStatus(text) {
    if (statusEl) statusEl.textContent = text;
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
    try {
      if (room) {
        room.disconnect();
      }
    } catch (e) {}
    window.location.href = returnUrl;
  }

  if (btnMic) btnMic.addEventListener('click', function () { toggleMic().catch(function () {}); });
  if (btnCam) btnCam.addEventListener('click', function () { toggleCam().catch(function () {}); });
  if (btnShare) btnShare.addEventListener('click', function () { toggleScreenShare().catch(function () {}); });
  if (btnEndMeeting) btnEndMeeting.addEventListener('click', function () { leaveMeeting(); });

  window.addEventListener('beforeunload', function () {
    try {
      if (room) room.disconnect();
    } catch (e) {}
  });

  connectRoom().catch(function (error) {
    console.error(error);
    setStatus(error && error.message ? error.message : 'تعذر بدء الاجتماع');
  });
})();
