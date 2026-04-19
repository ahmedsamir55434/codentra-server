const API = '';

const el = (id) => document.getElementById(id);

const state = {
  token: localStorage.getItem('nc_token') || '',
  me: null,
  activeView: 'auth',
  aiDraft: '',
  selectedChatId: null,
  postMediaDraft: [],
  storyMediaDraft: null,
  storyStyleDraft: { bg: '#0b0d10', color: '#ffffff', fontSize: 32, x: 0.5, y: 0.5, mediaScale: 1 },
  storyInteractiveDraft: null,
  createMode: 'post',
  isPublishing: false,
  otherUser: null,
  isAdmin: false,
  adminPostsFilter: 'all',
  adminOverview: null,
  adminUsersStats: [],
  adminRecentPosts: [],
  sounds: [],
  selectedSoundId: '',
  adminSounds: [],
  stories: [],
  myHighlights: [],
  otherHighlights: [],
};

let chatPollTimer = null;
let lastChatPollKey = '';
let reelsObserver = null;
let soundObserver = null;
let audioAutoplayUnlocked = false;
let storySoundEl = null;
let appPollTimer = null;
let appPollTick = 0;

let storyViewerState = {
  open: false,
  groups: [],
  groupIndex: 0,
  storyIndex: 0,
  timerId: null,
  rafId: 0,
  startedAt: 0,
  durationMs: 0,
  countdownTimerId: null,
  countdownEndAt: '',
};

let highlightViewerState = {
  open: false,
  highlight: null,
  index: 0,
};

const storyQuestionAnswersCache = new Map();
const storySliderResponsesCache = new Map();
const storyQuizAnswersCache = new Map();

let meetingState = {
  id: '',
  title: '',
  hostId: '',
  ws: null,
  approved: false,
  isHost: false,
  meId: '',
  iceServers: [],
  localStream: null,
  screenStream: null,
  sharingScreen: false,
  mainPeerId: '',
  chatMessages: [],
  mediaRecorder: null,
  recordedChunks: [],
  recordingUrl: '',
  recordingActive: false,
  pcs: new Map(),
  peerUsers: new Map(),
  remoteEls: new Map(),
  pending: new Map(),
  remoteMuted: false,
  openSeq: 0,
  opening: false,
};

function nextMeetingSeq() {
  meetingState.openSeq = (Number(meetingState.openSeq) || 0) + 1;
  return meetingState.openSeq;
}

function renderMeetingChat() {
  const list = el('meetingChatList');
  if (!list) return;
  const msgs = Array.isArray(meetingState.chatMessages) ? meetingState.chatMessages : [];
  list.innerHTML = '';
  msgs.forEach((m) => {
    const user = m.user || { username: 'Unknown' };
    const div = document.createElement('div');
    div.className = 'meetingChatMsg';
    div.innerHTML = `
      <div class="meetingChatMsg__meta">${escapeHtml(user.username || '')}${verifiedBadgeHtml(user.verified)} • ${escapeHtml(formatTime(m.createdAt || ''))}</div>
      <div class="meetingChatMsg__text">${escapeHtml(String(m.text || ''))}</div>
    `;
    list.appendChild(div);
  });
  try {
    list.scrollTop = list.scrollHeight;
  } catch {
    // ignore
  }
}

function pushMeetingChatMessage(message) {
  if (!message || typeof message !== 'object') return;
  if (!Array.isArray(meetingState.chatMessages)) meetingState.chatMessages = [];

  const id = String(message.id || '').trim();
  if (id && meetingState.chatMessages.some((m) => String(m?.id || '').trim() === id)) return;

  const clientId = String(message.clientId || '').trim();
  if (clientId) {
    const idx = meetingState.chatMessages.findIndex((m) => String(m?.clientId || '').trim() === clientId);
    if (idx !== -1) {
      meetingState.chatMessages[idx] = message;
      renderMeetingChat();
      return;
    }
  }

  meetingState.chatMessages.push(message);
  if (meetingState.chatMessages.length > 80) meetingState.chatMessages.splice(0, meetingState.chatMessages.length - 80);
  renderMeetingChat();
}

function showMeetingReaction(payload) {
  const wrap = el('meetingRemoteMainReaction');
  if (!wrap) return;
  const user = payload?.user || { username: '' };
  const raw = String(payload?.reaction || '').trim();
  const reaction = raw === 'hand' ? '✋' : raw;
  if (!reaction) return;
  try {
    wrap.textContent = `${String(user.username || '').trim() ? `${user.username}: ` : ''}${reaction}`;
    wrap.hidden = false;
  } catch {
    // ignore
  }
  window.setTimeout(() => {
    try {
      wrap.hidden = true;
      wrap.textContent = '';
    } catch {
      // ignore
    }
  }, 1800);
}

function updateMeetingRecordingUI() {
  const btn = el('meetingRecordBtn');
  const dl = el('meetingDownloadRecordingBtn');
  if (btn) btn.textContent = meetingState.recordingActive ? 'Stop recording' : 'Start recording';
  if (btn) btn.disabled = !meetingState.localStream || typeof MediaRecorder !== 'function';
  if (dl) dl.hidden = !meetingState.recordingUrl;
  if (dl && meetingState.recordingUrl) dl.href = meetingState.recordingUrl;
}

function updateMeetingChatUI() {
  const input = el('meetingChatInput');
  const btn = el('meetingChatSendBtn');
  const ws = meetingState.ws;
  const wsOpen = Boolean(ws && ws.readyState === 1);
  const canChat = Boolean(wsOpen && meetingState.approved);

  let placeholder = 'اكتب رسالة...';
  if (!meetingState.id) placeholder = 'افتح الميتنج أولاً...';
  else if (!ws) placeholder = 'جارٍ تجهيز الاتصال...';
  else if (!wsOpen) placeholder = 'جارٍ الاتصال...';
  else if (!meetingState.approved) placeholder = 'في انتظار الموافقة...';

  if (input) {
    input.disabled = !canChat;
    input.placeholder = placeholder;
  }
  if (btn) btn.disabled = !canChat;
}

function cleanupMeetingRecording() {
  try {
    if (meetingState.mediaRecorder && meetingState.mediaRecorder.state !== 'inactive') {
      meetingState.mediaRecorder.stop();
    }
  } catch {
    // ignore
  }
  meetingState.mediaRecorder = null;
  meetingState.recordedChunks = [];
  meetingState.recordingActive = false;
  if (meetingState.recordingUrl) {
    try {
      URL.revokeObjectURL(meetingState.recordingUrl);
    } catch {
      // ignore
    }
  }
  meetingState.recordingUrl = '';
  updateMeetingRecordingUI();
}

function getMeetingRecordingStream() {
  const base = meetingState.localStream;
  if (!base) return null;
  const out = new MediaStream();
  const a = base.getAudioTracks?.()[0] || null;
  if (a) out.addTrack(a);
  const vt = meetingState.sharingScreen
    ? meetingState.screenStream?.getVideoTracks?.()[0]
    : base.getVideoTracks?.()[0];
  if (vt) out.addTrack(vt);
  return out;
}

function startMeetingRecording() {
  if (typeof MediaRecorder !== 'function') {
    setAlert(el('meetingRoomAlert'), 'التسجيل غير مدعوم في هذا المتصفح.', 'danger');
    return;
  }
  if (meetingState.recordingActive) return;
  const stream = getMeetingRecordingStream();
  if (!stream) return;

  cleanupMeetingRecording();

  let rec;
  try {
    rec = new MediaRecorder(stream, { mimeType: 'video/webm;codecs=vp9,opus' });
  } catch {
    try {
      rec = new MediaRecorder(stream, { mimeType: 'video/webm;codecs=vp8,opus' });
    } catch {
      try {
        rec = new MediaRecorder(stream);
      } catch (err) {
        setAlert(el('meetingRoomAlert'), `فشل بدء التسجيل: ${err?.message || err}`, 'danger');
        return;
      }
    }
  }

  meetingState.mediaRecorder = rec;
  meetingState.recordedChunks = [];
  meetingState.recordingActive = true;
  updateMeetingRecordingUI();

  rec.ondataavailable = (ev) => {
    try {
      if (ev.data && ev.data.size > 0) meetingState.recordedChunks.push(ev.data);
    } catch {
      // ignore
    }
  };

  rec.onstop = () => {
    try {
      const blob = new Blob(meetingState.recordedChunks || [], { type: rec.mimeType || 'video/webm' });
      if (meetingState.recordingUrl) {
        try {
          URL.revokeObjectURL(meetingState.recordingUrl);
        } catch {
          // ignore
        }
      }
      meetingState.recordingUrl = URL.createObjectURL(blob);
    } catch {
      meetingState.recordingUrl = '';
    }
    meetingState.recordingActive = false;
    meetingState.mediaRecorder = null;
    updateMeetingRecordingUI();
  };

  try {
    rec.start(500);
  } catch {
    try {
      rec.start();
    } catch {
      meetingState.mediaRecorder = null;
      meetingState.recordingActive = false;
      updateMeetingRecordingUI();
    }
  }
}

function stopMeetingRecording() {
  const r = meetingState.mediaRecorder;
  if (!r) return;
  try {
    if (r.state !== 'inactive') r.stop();
  } catch {
    // ignore
  }
}

function isMeetingSeqActive(seq) {
  return Number(seq) === Number(meetingState.openSeq);
}

function clamp01(n) {
  const x = Number(n);
  if (!Number.isFinite(x)) return 0;
  return Math.max(0, Math.min(1, x));
}

function getMeetingIdFromUrl() {
  try {
    const u = new URL(window.location.href);
    const id = String(u.searchParams.get('meeting') || '').trim();
    return id;
  } catch {
    return '';
  }
}

function setMeetingIdInUrl(meetingId) {
  try {
    const u = new URL(window.location.href);
    if (meetingId) u.searchParams.set('meeting', String(meetingId));
    else u.searchParams.delete('meeting');
    window.history.replaceState({}, '', u.toString());
  } catch {
    // ignore
  }
}

function getWsUrl(meetingId) {
  const proto = window.location.protocol === 'https:' ? 'wss' : 'ws';
  const host = window.location.host;
  return `${proto}://${host}/ws?token=${encodeURIComponent(state.token)}&meetingId=${encodeURIComponent(String(meetingId || ''))}`;
}

function cleanupMeetingPeer(peerId) {
  const pid = String(peerId || '');
  const pc = meetingState.pcs.get(pid);
  if (pc) {
    try {
      pc.ontrack = null;
      pc.onicecandidate = null;
      pc.onconnectionstatechange = null;
      pc.close();
    } catch {
      // ignore
    }
  }
  meetingState.pcs.delete(pid);
  const elWrap = meetingState.remoteEls.get(pid);
  if (elWrap && elWrap.parentNode) {
    try {
      elWrap.parentNode.removeChild(elWrap);
    } catch {
      // ignore
    }
  }
  meetingState.remoteEls.delete(pid);
  meetingState.peerUsers.delete(pid);
}

function stopMeetingLocalStream() {
  const s = meetingState.localStream;
  meetingState.localStream = null;
  meetingState.sharingScreen = false;
  const ss = meetingState.screenStream;
  meetingState.screenStream = null;
  if (ss && typeof ss.getTracks === 'function') {
    try {
      ss.getTracks().forEach((t) => {
        try {
          t.stop();
        } catch {
          // ignore
        }
      });
    } catch {
      // ignore
    }
  }
  if (s && typeof s.getTracks === 'function') {
    try {
      s.getTracks().forEach((t) => {
        try {
          t.stop();
        } catch {
          // ignore
        }
      });
    } catch {
      // ignore
    }
  }

  const v = el('meetingLocalVideo');
  if (v instanceof HTMLVideoElement) {
    try {
      v.srcObject = null;
    } catch {
      // ignore
    }
  }

  const rv = el('meetingRemoteMainVideo');
  if (rv instanceof HTMLVideoElement) {
    try {
      rv.srcObject = null;
    } catch {
      // ignore
    }
  }
}

function stopMeeting() {
  nextMeetingSeq();
  meetingState.opening = false;
  try {
    setMeetingIdInUrl('');
  } catch {
    // ignore
  }

  try {
    if (meetingState.ws) meetingState.ws.close();
  } catch {
    // ignore
  }
  meetingState.ws = null;

  meetingState.pcs.forEach((pc) => {
    try {
      pc.close();
    } catch {
      // ignore
    }
  });
  meetingState.pcs.clear();
  meetingState.peerUsers.clear();
  meetingState.pending.clear();
  meetingState.remoteMuted = false;
  meetingState.mainPeerId = '';
  meetingState.chatMessages = [];

  const grid = el('meetingRemoteGrid');
  if (grid) grid.innerHTML = '';
  meetingState.remoteEls.clear();

  stopMeetingLocalStream();
  cleanupMeetingRecording();

  meetingState.id = '';
  meetingState.title = '';
  meetingState.hostId = '';
  meetingState.approved = false;
  meetingState.isHost = false;
  meetingState.meId = '';

  if (el('meetingRoomTitle')) el('meetingRoomTitle').textContent = 'لم يتم فتح ميتنج بعد.';
  if (el('meetingCopyLinkBtn')) el('meetingCopyLinkBtn').hidden = true;
  if (el('meetingHangupBtn')) el('meetingHangupBtn').hidden = true;
  if (el('meetingApprovalWrap')) el('meetingApprovalWrap').hidden = true;
  if (el('meetingPendingList')) el('meetingPendingList').innerHTML = '';

  el('meetingToggleCamBtn') && (el('meetingToggleCamBtn').disabled = true);
  el('meetingToggleMicBtn') && (el('meetingToggleMicBtn').disabled = true);
  el('meetingMuteRemoteBtn') && (el('meetingMuteRemoteBtn').disabled = true);
  el('meetingShareScreenBtn') && (el('meetingShareScreenBtn').disabled = true);
  el('meetingFullscreenBtn') && (el('meetingFullscreenBtn').disabled = true);
  el('meetingRecordBtn') && (el('meetingRecordBtn').disabled = true);
  const dl = el('meetingDownloadRecordingBtn');
  if (dl) dl.hidden = true;
  const chatList = el('meetingChatList');
  if (chatList) chatList.innerHTML = '';
  updateMeetingChatUI();
  const react = el('meetingRemoteMainReaction');
  if (react) react.hidden = true;
}

async function refreshMeetingDevicesUI() {
  const camSel = el('meetingCamSelect');
  const micSel = el('meetingMicSelect');
  if (!(camSel instanceof HTMLSelectElement) || !(micSel instanceof HTMLSelectElement)) return;
  if (!navigator.mediaDevices?.enumerateDevices) return;

  let devices = [];
  try {
    devices = await navigator.mediaDevices.enumerateDevices();
  } catch {
    devices = [];
  }

  const cams = devices.filter((d) => d.kind === 'videoinput');
  const mics = devices.filter((d) => d.kind === 'audioinput');
  const prevCam = String(camSel.value || '');
  const prevMic = String(micSel.value || '');

  camSel.innerHTML = '';
  cams.forEach((d, idx) => {
    const opt = document.createElement('option');
    opt.value = d.deviceId;
    opt.textContent = d.label || `Camera ${idx + 1}`;
    camSel.appendChild(opt);
  });

  micSel.innerHTML = '';
  mics.forEach((d, idx) => {
    const opt = document.createElement('option');
    opt.value = d.deviceId;
    opt.textContent = d.label || `Mic ${idx + 1}`;
    micSel.appendChild(opt);
  });

  if (prevCam && cams.some((d) => d.deviceId === prevCam)) camSel.value = prevCam;
  if (prevMic && mics.some((d) => d.deviceId === prevMic)) micSel.value = prevMic;
}

async function ensureMeetingLocalMedia() {
  const v = el('meetingLocalVideo');
  if (!(v instanceof HTMLVideoElement)) return;

  const camSel = el('meetingCamSelect');
  const micSel = el('meetingMicSelect');
  const camId = camSel instanceof HTMLSelectElement ? String(camSel.value || '') : '';
  const micId = micSel instanceof HTMLSelectElement ? String(micSel.value || '') : '';

  if (meetingState.opening) {
    setAlert(el('meetingRoomAlert'), 'جارٍ تشغيل الكاميرا/المايك... لو ظهرت رسالة صلاحيات وافق عليها.', '');
  }

  let stream;
  try {
    stream = await Promise.race([
      navigator.mediaDevices.getUserMedia({
      video: camId ? { deviceId: { exact: camId } } : true,
      audio: micId ? { deviceId: { exact: micId } } : true,
      }),
      new Promise((_, rej) =>
        window.setTimeout(() => rej(new Error('media_timeout')), 15000),
      ),
    ]);
  } catch (err) {
    const msg = String(err?.message || err);
    if (msg === 'media_timeout') {
      setAlert(el('meetingRoomAlert'), 'الوقت طال في تشغيل الكاميرا/المايك. تأكد من السماح بالصلاحيات ثم جرّب مرة أخرى.', 'danger');
    } else {
      setAlert(el('meetingRoomAlert'), `فشل تشغيل الكاميرا/المايك: ${msg}`, 'danger');
    }
    return;
  }

  stopMeetingLocalStream();
  meetingState.localStream = stream;
  try {
    v.srcObject = stream;
  } catch {
    // ignore
  }

  meetingState.pcs.forEach((pc) => {
    try {
      const senders = pc.getSenders ? pc.getSenders() : [];
      const vTrack = stream.getVideoTracks()[0] || null;
      const aTrack = stream.getAudioTracks()[0] || null;
      senders.forEach((s) => {
        try {
          if (s.track && s.track.kind === 'video' && vTrack) s.replaceTrack(vTrack);
          if (s.track && s.track.kind === 'audio' && aTrack) s.replaceTrack(aTrack);
        } catch {
          // ignore
        }
      });
    } catch {
      // ignore
    }
  });

  el('meetingToggleCamBtn') && (el('meetingToggleCamBtn').disabled = false);
  el('meetingToggleMicBtn') && (el('meetingToggleMicBtn').disabled = false);
  el('meetingMuteRemoteBtn') && (el('meetingMuteRemoteBtn').disabled = false);
  el('meetingShareScreenBtn') && (el('meetingShareScreenBtn').disabled = false);
  el('meetingFullscreenBtn') && (el('meetingFullscreenBtn').disabled = false);
  el('meetingRecordBtn') && (el('meetingRecordBtn').disabled = typeof MediaRecorder !== 'function');
  updateMeetingRecordingUI();
}

function updateMeetingControlLabels() {
  const camBtn = el('meetingToggleCamBtn');
  const micBtn = el('meetingToggleMicBtn');
  const muteBtn = el('meetingMuteRemoteBtn');
  const shareBtn = el('meetingShareScreenBtn');
  const fsBtn = el('meetingFullscreenBtn');
  const ls = meetingState.localStream;

  const videoEnabled = ls?.getVideoTracks?.()[0]?.enabled;
  const audioEnabled = ls?.getAudioTracks?.()[0]?.enabled;

  if (camBtn) camBtn.textContent = videoEnabled === false ? 'Cam Off' : 'Cam On';
  if (micBtn) micBtn.textContent = audioEnabled === false ? 'Mic Off' : 'Mic On';
  if (muteBtn) muteBtn.textContent = meetingState.remoteMuted ? 'Unmute remote' : 'Mute remote';
  if (shareBtn) shareBtn.textContent = meetingState.sharingScreen ? 'Stop share' : 'Share screen';
  if (fsBtn) fsBtn.textContent = 'Fullscreen';
}

function setMeetingMainPeer(peerId) {
  const pid = String(peerId || '').trim();
  meetingState.mainPeerId = pid;

  meetingState.remoteEls.forEach((wrap, k) => {
    try {
      wrap.classList.toggle('meetingRemoteItem--active', String(k) === pid);
    } catch {
      // ignore
    }
  });

  const wrap = meetingState.remoteEls.get(pid);
  const srcV = wrap ? wrap.querySelector('[data-peer-video]') : null;
  const mainV = el('meetingRemoteMainVideo');
  if (!(mainV instanceof HTMLVideoElement)) return;
  if (!(srcV instanceof HTMLVideoElement)) return;
  try {
    mainV.srcObject = srcV.srcObject || null;
    mainV.muted = Boolean(meetingState.remoteMuted);
  } catch {
    // ignore
  }
}

async function startMeetingScreenShare() {
  if (!meetingState.localStream) return;
  if (!navigator.mediaDevices?.getDisplayMedia) {
    setAlert(el('meetingRoomAlert'), 'مشاركة الشاشة توفرها حسب المتصفح. جرّب Chrome/Edge.', 'danger');
    return;
  }

  let screenStream;
  try {
    screenStream = await navigator.mediaDevices.getDisplayMedia({ video: true, audio: false });
  } catch (err) {
    setAlert(el('meetingRoomAlert'), `فشل مشاركة الشاشة: ${err?.message || err}`, 'danger');
    return;
  }

  const screenTrack = screenStream.getVideoTracks?.()[0] || null;
  if (!screenTrack) {
    setAlert(el('meetingRoomAlert'), 'فشل الحصول على Screen track.', 'danger');
    try {
      screenStream.getTracks().forEach((t) => t.stop());
    } catch {
      // ignore
    }
    return;
  }

  meetingState.screenStream = screenStream;
  meetingState.sharingScreen = true;
  updateMeetingControlLabels();

  const v = el('meetingLocalVideo');
  if (v instanceof HTMLVideoElement) {
    try {
      v.srcObject = screenStream;
    } catch {
      // ignore
    }
  }

  meetingState.pcs.forEach((pc) => {
    try {
      const senders = pc.getSenders ? pc.getSenders() : [];
      senders.forEach((s) => {
        try {
          if (s.track && s.track.kind === 'video') s.replaceTrack(screenTrack);
        } catch {
          // ignore
        }
      });
    } catch {
      // ignore
    }
  });

  screenTrack.onended = () => {
    try {
      stopMeetingScreenShare();
    } catch {
      // ignore
    }
  };
}

function stopMeetingScreenShare() {
  if (!meetingState.sharingScreen) return;
  meetingState.sharingScreen = false;
  updateMeetingControlLabels();

  const cameraTrack = meetingState.localStream?.getVideoTracks?.()[0] || null;
  meetingState.pcs.forEach((pc) => {
    try {
      const senders = pc.getSenders ? pc.getSenders() : [];
      senders.forEach((s) => {
        try {
          if (s.track && s.track.kind === 'video' && cameraTrack) s.replaceTrack(cameraTrack);
        } catch {
          // ignore
        }
      });
    } catch {
      // ignore
    }
  });

  const v = el('meetingLocalVideo');
  if (v instanceof HTMLVideoElement) {
    try {
      v.srcObject = meetingState.localStream;
    } catch {
      // ignore
    }
  }

  const ss = meetingState.screenStream;
  meetingState.screenStream = null;
  if (ss && typeof ss.getTracks === 'function') {
    try {
      ss.getTracks().forEach((t) => {
        try {
          t.stop();
        } catch {
          // ignore
        }
      });
    } catch {
      // ignore
    }
  }
}

function meetingSend(payload) {
  try {
    if (meetingState.ws && meetingState.ws.readyState === 1) {
      meetingState.ws.send(JSON.stringify(payload));
    }
  } catch {
    // ignore
  }
}

function renderMeetingPendingList() {
  const wrap = el('meetingApprovalWrap');
  const list = el('meetingPendingList');
  if (!wrap || !list) return;
  if (!meetingState.isHost) {
    wrap.hidden = true;
    list.innerHTML = '';
    return;
  }

  const pending = Array.from(meetingState.pending.values());
  wrap.hidden = pending.length === 0;
  list.innerHTML = '';
  pending.forEach((p) => {
    const u = p.user || { username: 'Unknown' };
    const div = document.createElement('div');
    div.className = 'item';
    div.innerHTML = `
      <div class="item__top">
        <div>
          <div class="item__title">${escapeHtml(u.username || '')}${verifiedBadgeHtml(u.verified)}</div>
          <div class="item__sub">طلب دخول للميتينج</div>
        </div>
        <div class="row">
          <button class="btn btn--primary" type="button" data-approve>Approve</button>
          <button class="btn" type="button" data-reject>Reject</button>
        </div>
      </div>
    `;
    div.querySelector('[data-approve]')?.addEventListener('click', () => {
      meetingSend({ type: 'approve', requestId: p.requestId });
      meetingState.pending.delete(p.requestId);
      renderMeetingPendingList();
    });
    div.querySelector('[data-reject]')?.addEventListener('click', () => {
      meetingSend({ type: 'reject', requestId: p.requestId });
      meetingState.pending.delete(p.requestId);
      renderMeetingPendingList();
    });
    list.appendChild(div);
  });
}

function renderMeetingRemotePeer(peerId) {
  const pid = String(peerId || '').trim();
  if (!pid) return;
  if (meetingState.remoteEls.has(pid)) return;
  const grid = el('meetingRemoteGrid');
  if (!grid) return;
  const u = meetingState.peerUsers.get(pid) || { username: pid };

  const wrap = document.createElement('div');
  wrap.className = 'meetingRemoteItem';
  wrap.innerHTML = `
    <div class="muted meetingRemoteName">${escapeHtml(u.username || '')}${verifiedBadgeHtml(u.verified)}</div>
    <video autoplay playsinline class="meetingVideo" data-peer-video></video>
  `;

  wrap.addEventListener('click', () => {
    setMeetingMainPeer(pid);
  });

  wrap.addEventListener('dblclick', async () => {
    const mainV = el('meetingRemoteMainVideo');
    if (!(mainV instanceof HTMLVideoElement)) return;
    try {
      if (typeof mainV.requestFullscreen === 'function') {
        await mainV.requestFullscreen();
      }
    } catch {
      // ignore
    }
  });

  const v = wrap.querySelector('[data-peer-video]');
  if (v instanceof HTMLVideoElement) {
    v.muted = Boolean(meetingState.remoteMuted);
  }
  grid.appendChild(wrap);
  meetingState.remoteEls.set(pid, wrap);
}

function setRemoteVideoStream(peerId, stream) {
  const pid = String(peerId || '').trim();
  renderMeetingRemotePeer(pid);
  const wrap = meetingState.remoteEls.get(pid);
  if (!wrap) return;
  const v = wrap.querySelector('[data-peer-video]');
  if (!(v instanceof HTMLVideoElement)) return;
  try {
    v.srcObject = stream;
    v.muted = Boolean(meetingState.remoteMuted);
  } catch {
    // ignore
  }

  if (!meetingState.mainPeerId) {
    setMeetingMainPeer(pid);
  } else if (meetingState.mainPeerId === pid) {
    setMeetingMainPeer(pid);
  }
}

function ensurePeerConnection(peerId) {
  const pid = String(peerId || '').trim();
  if (!pid) return null;
  if (meetingState.pcs.has(pid)) return meetingState.pcs.get(pid);

  if (typeof RTCPeerConnection !== 'function') {
    setAlert(el('meetingRoomAlert'), 'المتصفح لا يدعم WebRTC.', 'danger');
    return null;
  }

  let pc;
  try {
    pc = new RTCPeerConnection({ iceServers: meetingState.iceServers || [] });
  } catch (err) {
    setAlert(el('meetingRoomAlert'), `فشل إنشاء اتصال WebRTC: ${err?.message || err}`, 'danger');
    return null;
  }

  try {
    const ls = meetingState.localStream;
    if (ls && typeof ls.getTracks === 'function') {
      ls.getTracks().forEach((t) => {
        try {
          pc.addTrack(t, ls);
        } catch {
          // ignore
        }
      });
    }
  } catch {
    // ignore
  }

  pc.onicecandidate = (ev) => {
    if (!ev.candidate) return;
    meetingSend({ type: 'signal', to: pid, data: { kind: 'ice', candidate: ev.candidate } });
  };

  pc.ontrack = (ev) => {
    const stream = ev.streams && ev.streams[0] ? ev.streams[0] : null;
    if (!stream) return;
    setRemoteVideoStream(pid, stream);
  };

  pc.onconnectionstatechange = () => {
    const st = String(pc.connectionState || '');
    if (st === 'failed' || st === 'closed' || st === 'disconnected') {
      cleanupMeetingPeer(pid);
    }
  };

  meetingState.pcs.set(pid, pc);
  return pc;
}

async function maybeMakeOffer(peerId) {
  const pid = String(peerId || '').trim();
  if (!pid) return;
  if (!meetingState.meId) return;
  const initiate = String(meetingState.meId) < pid;
  if (!initiate) return;

  const pc = ensurePeerConnection(pid);
  if (!pc) return;

  try {
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    meetingSend({ type: 'signal', to: pid, data: { kind: 'offer', sdp: pc.localDescription } });
  } catch {
    // ignore
  }
}

async function handleMeetingSignal(from, data) {
  const pid = String(from || '').trim();
  if (!pid) return;
  const pc = ensurePeerConnection(pid);
  if (!pc) return;

  const kind = String(data?.kind || '').trim();
  if (kind === 'offer' && data?.sdp) {
    try {
      await pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
      const ans = await pc.createAnswer();
      await pc.setLocalDescription(ans);
      meetingSend({ type: 'signal', to: pid, data: { kind: 'answer', sdp: pc.localDescription } });
    } catch {
      // ignore
    }
    return;
  }

  if (kind === 'answer' && data?.sdp) {
    try {
      await pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
    } catch {
      // ignore
    }
    return;
  }

  if (kind === 'ice' && data?.candidate) {
    try {
      await pc.addIceCandidate(new RTCIceCandidate(data.candidate));
    } catch {
      // ignore
    }
  }
}

async function openMeeting(meetingId) {
  if (!requireToken()) return;
  const id = String(meetingId || '').trim();
  if (!id) return;

  if (meetingState.opening && meetingState.id === id) return;

  setAlert(el('meetingRoomAlert'), '', '');

  stopMeeting();
  showView('meetings');

  meetingState.opening = true;
  const seq = nextMeetingSeq();
  if (isMeetingSeqActive(seq)) setAlert(el('meetingRoomAlert'), 'جارٍ فتح الميتنج...', '');

  let meeting;
  try {
    const rtc = await apiFetch('/rtc/config', { timeoutMs: 5000 });
    meetingState.iceServers = Array.isArray(rtc.iceServers) ? rtc.iceServers : [];
  } catch {
    meetingState.iceServers = [];
  }

  if (!isMeetingSeqActive(seq)) return;

  try {
    const data = await apiFetch(`/meetings/${encodeURIComponent(id)}`, { timeoutMs: 7000 });
    meeting = data.meeting;
  } catch (err) {
    setAlert(el('meetingRoomAlert'), `تعذر فتح الميتنج: ${err.data?.error || err.message}`, 'danger');
    meetingState.opening = false;
    return;
  }

  if (!isMeetingSeqActive(seq)) return;

  meetingState.id = String(meeting.id || id);
  meetingState.title = String(meeting.title || '');
  meetingState.hostId = String(meeting.hostId || meeting.host?.id || '');
  setMeetingIdInUrl(meetingState.id);

  if (el('meetingRoomTitle')) {
    const t = meetingState.title ? `${meetingState.title} • ` : '';
    el('meetingRoomTitle').textContent = `${t}${meetingState.id}`;
  }

  el('meetingCopyLinkBtn') && (el('meetingCopyLinkBtn').hidden = false);
  el('meetingHangupBtn') && (el('meetingHangupBtn').hidden = false);

  await refreshMeetingDevicesUI();
  if (!isMeetingSeqActive(seq)) return;
  await ensureMeetingLocalMedia();
  if (!isMeetingSeqActive(seq)) return;
  updateMeetingControlLabels();
  updateMeetingRecordingUI();
  if (!meetingState.localStream) {
    meetingState.opening = false;
    return;
  }

  let ws;
  try {
    ws = new WebSocket(getWsUrl(meetingState.id));
  } catch (err) {
    setAlert(el('meetingRoomAlert'), `فشل الاتصال: ${err?.message || err}`, 'danger');
    meetingState.opening = false;
    return;
  }

  if (!isMeetingSeqActive(seq)) {
    try {
      ws.close();
    } catch {
      // ignore
    }
    return;
  }

  meetingState.ws = ws;
  setAlert(el('meetingRoomAlert'), 'جارٍ الاتصال بالسيرفر...', '');
  updateMeetingChatUI();

  const wsTimeout = window.setTimeout(() => {
    try {
      if (!isMeetingSeqActive(seq)) return;
      if (!meetingState.ws || meetingState.ws.readyState !== 1) {
        setAlert(el('meetingRoomAlert'), 'الاتصال تأخر. حاول مرة أخرى.', 'danger');
        try {
          meetingState.ws && meetingState.ws.close();
        } catch {
          // ignore
        }
      }
    } catch {
      // ignore
    }
  }, 8000);

  ws.onopen = () => {
    try {
      if (!isMeetingSeqActive(seq)) return;
      setAlert(el('meetingRoomAlert'), 'تم الاتصال. في انتظار الحالة...', '');
      updateMeetingChatUI();
    } catch {
      // ignore
    }
  };

  ws.onerror = () => {
    try {
      if (!isMeetingSeqActive(seq)) return;
      setAlert(el('meetingRoomAlert'), 'حصل خطأ في الاتصال. جرّب مرة أخرى.', 'danger');
    } catch {
      // ignore
    }
  };

  ws.onmessage = async (ev) => {
    if (!isMeetingSeqActive(seq)) return;
    let msg;
    try {
      msg = JSON.parse(String(ev.data || '{}'));
    } catch {
      return;
    }
    const type = String(msg.type || '').trim();
    if (!type) return;

    if (type === 'state') {
      try {
        clearTimeout(wsTimeout);
      } catch {
        // ignore
      }
      meetingState.approved = Boolean(msg.approved);
      meetingState.isHost = Boolean(msg.isHost);
      meetingState.meId = String(msg.me?.id || '');
      meetingState.pending.clear();
      renderMeetingPendingList();
      updateMeetingChatUI();

      if (Array.isArray(msg.chatHistory)) {
        meetingState.chatMessages = msg.chatHistory;
        renderMeetingChat();
      }

      if (!meetingState.approved) {
        setAlert(el('meetingRoomAlert'), 'تم إرسال طلب دخول.. انتظر موافقة صاحب الميتنج.', '');
        return;
      }

      setAlert(el('meetingRoomAlert'), '', '');
      const peers = Array.isArray(msg.peers) ? msg.peers : [];
      peers.forEach((u) => {
        const pid = String(u?.id || '').trim();
        if (!pid) return;
        meetingState.peerUsers.set(pid, u);
        ensurePeerConnection(pid);
      });
      for (const u of peers) {
        const pid = String(u?.id || '').trim();
        if (!pid) continue;
        await maybeMakeOffer(pid);
      }
      return;
    }

    if (type === 'join_request') {
      if (!meetingState.isHost) return;
      const requestId = String(msg.requestId || '').trim();
      const user = msg.user || null;
      if (!requestId || !user?.id) return;
      meetingState.pending.set(requestId, { requestId, user });
      renderMeetingPendingList();
      return;
    }

    if (type === 'join_approved') {
      meetingState.approved = true;
      setAlert(el('meetingRoomAlert'), '', '');
      updateMeetingChatUI();
      if (Array.isArray(msg.chatHistory)) {
        meetingState.chatMessages = msg.chatHistory;
        renderMeetingChat();
      }
      const peers = Array.isArray(msg.peers) ? msg.peers : [];
      peers.forEach((u) => {
        const pid = String(u?.id || '').trim();
        if (!pid) return;
        meetingState.peerUsers.set(pid, u);
        ensurePeerConnection(pid);
      });
      for (const u of peers) {
        const pid = String(u?.id || '').trim();
        if (!pid) continue;
        await maybeMakeOffer(pid);
      }
      return;
    }

    if (type === 'chat_message') {
      const m = msg.message || null;
      if (m) pushMeetingChatMessage(m);
      return;
    }

    if (type === 'reaction') {
      showMeetingReaction(msg);
      return;
    }

    if (type === 'join_rejected') {
      setAlert(el('meetingRoomAlert'), 'تم رفض طلب الدخول.', 'danger');
      stopMeeting();
      return;
    }

    if (type === 'peer_joined') {
      const u = msg.peer || null;
      const pid = String(u?.id || '').trim();
      if (!pid) return;
      meetingState.peerUsers.set(pid, u);
      ensurePeerConnection(pid);
      await maybeMakeOffer(pid);
      return;
    }

    if (type === 'peer_left') {
      cleanupMeetingPeer(String(msg.peerId || ''));
      return;
    }

    if (type === 'signal') {
      await handleMeetingSignal(String(msg.from || ''), msg.data || {});
      return;
    }

    if (type === 'meeting_ended') {
      setAlert(el('meetingRoomAlert'), 'انتهى الميتنج.', 'danger');
      stopMeeting();
      return;
    }
  };

  ws.onclose = () => {
    try {
      clearTimeout(wsTimeout);
    } catch {
      // ignore
    }
    if (meetingState.id) setAlert(el('meetingRoomAlert'), 'تم قطع الاتصال بالميتينج.', 'danger');
    meetingState.opening = false;
  };
}

function getStoryStyleFromUI() {
  const bg = String(el('storyBg')?.value || state.storyStyleDraft.bg || '#0b0d10');
  const color = String(el('storyColor')?.value || state.storyStyleDraft.color || '#ffffff');
  const fontSize = Number(el('storyFontSize')?.value || state.storyStyleDraft.fontSize || 32);
  const mediaScale = Number(el('storyMediaScale')?.value || state.storyStyleDraft.mediaScale || 1);
  return {
    ...state.storyStyleDraft,
    bg,
    color,
    fontSize: Number.isFinite(fontSize) ? fontSize : 32,
    mediaScale: Number.isFinite(mediaScale) ? mediaScale : 1,
  };
}

function renderStoryStage() {
  const stage = el('storyStage');
  const mediaBox = el('storyStageMedia');
  const textBox = el('storyStageText');
  const stickerBox = el('storyStageSticker');
  if (!stage || !mediaBox || !textBox) return;

  const st = getStoryStyleFromUI();
  stage.style.background = st.bg;

  mediaBox.innerHTML = '';
  const media = state.storyMediaDraft;
  if (media?.url) {
    const t = String(media.type || '');
    if (t.startsWith('video')) {
      const v = document.createElement('video');
      v.src = media.url;
      v.muted = true;
      v.playsInline = true;
      v.autoplay = true;
      v.loop = true;
      v.controls = false;
      v.style.transform = `scale(${st.mediaScale})`;
      try {
        void v.play();
      } catch {
        // ignore
      }
      mediaBox.appendChild(v);
    } else {
      const img = document.createElement('img');
      img.alt = '';
      img.src = media.url;
      img.style.transform = `scale(${st.mediaScale})`;
      mediaBox.appendChild(img);
    }
  }

  const storyText = String(el('storyText')?.value || '').trimEnd();
  textBox.textContent = storyText;
  textBox.style.color = st.color;
  textBox.style.fontSize = `${Math.round(st.fontSize)}px`;
  textBox.style.left = `${Math.round(clamp01(st.x) * 100)}%`;
  textBox.style.top = `${Math.round(clamp01(st.y) * 100)}%`;
  textBox.hidden = !storyText.trim();

  try {
    renderStoryStageStickerPreview(stickerBox);
  } catch {
    // ignore
  }
}

function getStoryInteractiveDraftFromUI() {
  const kind = String(el('storyInteractiveKind')?.value || '').trim().toLowerCase();
  if (!kind) return null;

  const question = String(el('storyInteractiveQuestion')?.value || '').trim();
  const pollOpts = [
    String(el('storyPollOpt1')?.value || '').trim(),
    String(el('storyPollOpt2')?.value || '').trim(),
    String(el('storyPollOpt3')?.value || '').trim(),
    String(el('storyPollOpt4')?.value || '').trim(),
  ].filter(Boolean);

  if (kind === 'poll') {
    if (!question) return { kind, question: '' };
    return { kind: 'poll', question, options: pollOpts };
  }

  if (kind === 'question') {
    if (!question) return { kind, question: '' };
    return { kind: 'question', question };
  }

  if (kind === 'quiz') {
    if (!question) return { kind, question: '' };
    const rawCorrectIndex = String(el('storyQuizCorrectIndex')?.value || '').trim();
    const correctIndex = rawCorrectIndex === '' ? null : Number(rawCorrectIndex);
    const out = { kind: 'quiz', question, options: pollOpts };
    if (Number.isFinite(correctIndex)) out.correctIndex = correctIndex;
    return out;
  }

  if (kind === 'slider') {
    if (!question) return { kind, question: '' };
    const emoji = String(el('storySliderEmoji')?.value || '❤️').trim() || '❤️';
    return { kind: 'slider', question, emoji };
  }

  if (kind === 'link') {
    const title = String(el('storyLinkTitle')?.value || '').trim();
    const url = String(el('storyLinkUrl')?.value || '').trim();
    return { kind: 'link', title, url };
  }

  if (kind === 'mention') {
    const username = String(el('storyMentionUsername')?.value || '').trim();
    return { kind: 'mention', username };
  }

  if (kind === 'location') {
    const name = String(el('storyLocationName')?.value || '').trim();
    return { kind: 'location', name };
  }

  if (kind === 'countdown') {
    const title = String(el('storyCountdownTitle')?.value || '').trim();
    const rawEndAt = String(el('storyCountdownEndAt')?.value || '').trim();
    let endAt = '';
    try {
      if (rawEndAt) endAt = new Date(rawEndAt).toISOString();
    } catch {
      endAt = '';
    }
    return { kind: 'countdown', title, endAt };
  }

  return { kind };
}

function renderStoryStageStickerPreview(stickerBox) {
  if (!stickerBox) return;
  const d = getStoryInteractiveDraftFromUI();
  state.storyInteractiveDraft = d;
  if (!d || !d.kind) {
    stickerBox.hidden = true;
    stickerBox.innerHTML = '';
    return;
  }

  const kind = String(d.kind || '').trim().toLowerCase();
  const q = String(d.question || '').trim();

  if (kind === 'poll' || kind === 'quiz' || kind === 'question' || kind === 'slider') {
    if (!q) {
      stickerBox.hidden = false;
      stickerBox.innerHTML = `<div class="muted" style="color:rgba(255,255,255,.82)">اكتب السؤال لعرض الـ Sticker</div>`;
      return;
    }
  }

  if (kind === 'poll' || kind === 'quiz') {
    const opts = Array.isArray(d.options) ? d.options : [];
    stickerBox.hidden = false;
    stickerBox.innerHTML = `
      <div class="storyStickerTitle">${escapeHtml(q)}</div>
      <div class="stack">
        ${opts
          .slice(0, 4)
          .map((t) => `<button class="storyStickerOption" type="button" disabled>${escapeHtml(String(t || ''))}</button>`)
          .join('')}
      </div>
      <div class="storyStickerMeta">${kind === 'quiz' ? 'Quiz Preview' : 'Poll Preview'}</div>
    `;
    return;
  }

  if (kind === 'question') {
    stickerBox.hidden = false;
    stickerBox.innerHTML = `
      <div class="storyStickerTitle">${escapeHtml(q)}</div>
      <div class="muted" style="color:rgba(255,255,255,.82)">إجابة كتابة</div>
      <div class="storyStickerMeta">Preview</div>
    `;
    return;
  }

  if (kind === 'slider') {
    const emoji = String(d.emoji || '❤️').trim() || '❤️';
    stickerBox.hidden = false;
    stickerBox.innerHTML = `
      <div class="storyStickerTitle">${escapeHtml(q)}</div>
      <div class="muted" style="color:rgba(255,255,255,.82)">${escapeHtml(emoji)} Slider</div>
      <div class="storyStickerMeta">Preview</div>
    `;
    return;
  }

  if (kind === 'link') {
    const url = String(d.url || '').trim();
    stickerBox.hidden = false;
    stickerBox.innerHTML = `
      <div class="storyStickerTitle">${escapeHtml(String(d.title || 'Link'))}</div>
      <div class="muted" style="color:rgba(255,255,255,.82)">${escapeHtml(url || 'https://...')}</div>
      <div class="storyStickerMeta">Preview</div>
    `;
    return;
  }

  if (kind === 'mention') {
    const username = String(d.username || '').trim();
    stickerBox.hidden = false;
    stickerBox.innerHTML = `
      <div class="storyStickerTitle">Mention</div>
      <div class="muted" style="color:rgba(255,255,255,.82)">@${escapeHtml(username.replace(/^@/, ''))}</div>
      <div class="storyStickerMeta">Preview</div>
    `;
    return;
  }

  if (kind === 'location') {
    const name = String(d.name || '').trim();
    stickerBox.hidden = false;
    stickerBox.innerHTML = `
      <div class="storyStickerTitle">Location</div>
      <div class="muted" style="color:rgba(255,255,255,.82)">${escapeHtml(name || '...')}</div>
      <div class="storyStickerMeta">Preview</div>
    `;
    return;
  }

  if (kind === 'countdown') {
    stickerBox.hidden = false;
    stickerBox.innerHTML = `
      <div class="storyStickerTitle">${escapeHtml(String(d.title || 'Countdown'))}</div>
      <div class="muted" style="color:rgba(255,255,255,.82)">${escapeHtml(String(d.endAt || ''))}</div>
      <div class="storyStickerMeta">Preview</div>
    `;
    return;
  }
}
function formatDurationShort(ms) {
  const n = Math.max(0, Number(ms || 0));
  const totalSec = Math.floor(n / 1000);
  const d = Math.floor(totalSec / 86400);
  const h = Math.floor((totalSec % 86400) / 3600);
  const m = Math.floor((totalSec % 3600) / 60);
  const s = totalSec % 60;
  if (d > 0) return `${d}d ${h}h`;
  if (h > 0) return `${h}h ${m}m`;
  return `${m}m ${s}s`;
}

function renderStoryCanvas(target, story) {
  if (!target) return;
  const s = story && typeof story === 'object' ? story : {};
  const st = s.style && typeof s.style === 'object' ? s.style : {};
  const style = {
    bg: typeof st.bg === 'string' ? st.bg : '#0b0d10',
    color: typeof st.color === 'string' ? st.color : '#ffffff',
    fontSize: Number.isFinite(Number(st.fontSize)) ? Number(st.fontSize) : 32,
    x: clamp01(st.x ?? 0.5),
    y: clamp01(st.y ?? 0.5),
    mediaScale: Number.isFinite(Number(st.mediaScale)) ? Number(st.mediaScale) : 1,
  };

  target.innerHTML = '';
  const canvas = document.createElement('div');
  canvas.className = 'storyViewCanvas';
  canvas.style.background = style.bg;
  const mediaBox = document.createElement('div');
  mediaBox.className = 'storyViewCanvas__media';
  mediaBox.style.transform = `scale(${style.mediaScale})`;
  const textBox = document.createElement('div');
  textBox.className = 'storyViewCanvas__text';
  textBox.style.color = style.color;
  textBox.style.fontSize = `${Math.round(style.fontSize)}px`;
  textBox.style.left = `${Math.round(style.x * 100)}%`;
  textBox.style.top = `${Math.round(style.y * 100)}%`;
  textBox.textContent = String(s.text || '');
  textBox.hidden = !String(s.text || '').trim();

  const stickerBox = document.createElement('div');
  stickerBox.className = 'storyViewCanvas__sticker';
  stickerBox.hidden = true;

  const type = String(s?.media?.type || '');
  const url = String(s?.media?.url || '');
  if (type.startsWith('video') && url) {
    const v = document.createElement('video');
    v.src = url;
    v.autoplay = true;
    v.playsInline = true;
    v.muted = true;
    v.controls = false;
    v.preload = 'metadata';
    mediaBox.appendChild(v);
    canvas.appendChild(mediaBox);
    canvas.appendChild(textBox);
    canvas.appendChild(stickerBox);
    renderStoryInteractiveSticker(stickerBox, s);
    target.appendChild(canvas);
    return { kind: 'video', el: v };
  }

  if (url) {
    const img = document.createElement('img');
    img.src = url;
    img.alt = '';
    mediaBox.appendChild(img);
  }
  canvas.appendChild(mediaBox);
  canvas.appendChild(textBox);
  canvas.appendChild(stickerBox);
  renderStoryInteractiveSticker(stickerBox, s);
  target.appendChild(canvas);
  return { kind: url ? 'image' : 'text', el: null };
}

function renderStoryInteractiveSticker(container, story) {
  if (!container) return;
  const s = story && typeof story === 'object' ? story : {};
  const inter = s.interactive && typeof s.interactive === 'object' ? s.interactive : null;

  try {
    container.onclick = (e) => {
      try {
        e.preventDefault();
      } catch {
        // ignore
      }
      try {
        e.stopPropagation();
      } catch {
        // ignore
      }
    };
  } catch {
    // ignore
  }

  if (!inter || !String(inter.kind || '').trim()) {
    container.hidden = true;
    container.innerHTML = '';
    return;
  }

  const kind = String(inter.kind || '').trim().toLowerCase();
  const storyId = String(s.sourceStoryId || s.id || '');
  const q = String(inter.question || '').trim();
  if (['poll', 'question', 'quiz', 'slider'].includes(kind) && !q) {
    container.hidden = true;
    container.innerHTML = '';
    return;
  }

  if (kind === 'poll') {
    const options = Array.isArray(inter.options) ? inter.options : [];
    const canVote = Boolean(inter.canVote);
    const showResults = Boolean(inter.isOwner) || !canVote;
    const myVote = String(inter.myVoteOptionId || '');

    container.hidden = false;
    container.innerHTML = `
      <div class="storyStickerTitle">${escapeHtml(q)}</div>
      <div class="stack" id="storyStickerPollOpts"></div>
      <div class="storyStickerMeta">${showResults ? `إجمالي: ${escapeHtml(String(inter.totalVotes ?? 0))} صوت` : 'اضغط للتصويت'}</div>
    `;

    const list = container.querySelector('#storyStickerPollOpts');
    if (!(list instanceof HTMLElement)) return;

    options.forEach((o) => {
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'storyStickerOption';
      const oid = String(o?.id || '');
      const label = String(o?.text || '');
      const pct = Number(o?.pct || 0);
      const count = Number(o?.count || 0);
      const picked = myVote && oid && myVote === oid;
      btn.disabled = !canVote;
      btn.innerHTML = `
        <div style="text-align:right; flex:1">
          <div style="font-weight:800">${escapeHtml(label)}${picked ? ' ✓' : ''}</div>
          ${showResults ? `<div class="storyStickerBar"><div class="storyStickerBarFill" style="width:${Math.max(0, Math.min(100, pct))}%"></div></div>` : ''}
        </div>
        ${showResults ? `<div style="min-width:64px; text-align:left; font-weight:900">${escapeHtml(String(pct))}%</div>` : ''}
      `;

      if (canVote) {
        btn.addEventListener('click', async () => {
          const prevInteractive = s.interactive;
          try {
            const current = prevInteractive && typeof prevInteractive === 'object' ? prevInteractive : null;
            if (current) {
              const prevTotal = Number(current.totalVotes || 0);
              const nextTotal = prevTotal + 1;
              const nextOptions = (Array.isArray(current.options) ? current.options : []).map((opt) => {
                const id = String(opt?.id || '');
                const baseCount = Number(opt?.count || 0);
                const nextCount = id && id === oid ? baseCount + 1 : baseCount;
                const nextPct = nextTotal ? Math.round((nextCount / nextTotal) * 100) : 0;
                return { ...opt, count: nextCount, pct: nextPct };
              });

              s.interactive = {
                ...current,
                canVote: false,
                myVoteOptionId: oid,
                totalVotes: nextTotal,
                options: nextOptions,
              };
              renderStoryInteractiveSticker(container, s);
            }
          } catch {
            // ignore optimistic rendering errors
          }

          try {
            const data = await apiFetch(`/stories/${encodeURIComponent(storyId)}/poll/vote`, {
              method: 'POST',
              body: JSON.stringify({ optionId: oid }),
            });
            s.interactive = data?.interactive || s.interactive;
            renderStoryInteractiveSticker(container, s);
          } catch (err) {
            s.interactive = prevInteractive;
            renderStoryInteractiveSticker(container, s);
            setAlert(el('storyCommentsAlert'), `فشل التصويت: ${err.data?.error || err.message}`, 'danger');
          }
        });
      }

      list.appendChild(btn);
    });

    return;
  }

  if (kind === 'question') {
    const canAnswer = Boolean(inter.canAnswer);
    const answeredByMe = Boolean(inter.answeredByMe);
    const answersCount = Number(inter.answersCount || 0);
    const isOwner = Boolean(inter.isOwner);

    const cached = storyId ? storyQuestionAnswersCache.get(storyId) : null;
    const answersHtml =
      isOwner && cached && Array.isArray(cached.answers)
        ? `
          <div class="divider" style="margin:10px 0; border-color:rgba(255,255,255,.14)"></div>
          <div class="storyStickerMeta">${escapeHtml(String(cached.question || q))}</div>
          <div class="stack">
            ${cached.answers
              .slice(0, 40)
              .map((a) => {
                const u = a.user || { username: 'Unknown' };
                return `<div style="padding:8px 10px; border:1px solid rgba(255,255,255,.16); border-radius:12px; background:rgba(255,255,255,.06)"><strong>${escapeHtml(u.username || '')}${verifiedBadgeHtml(u.verified)}</strong><div>${escapeHtml(String(a.text || ''))}</div></div>`;
              })
              .join('')}
          </div>
        `
        : '';

    container.hidden = false;
    container.innerHTML = `
      <div class="storyStickerTitle">${escapeHtml(q)}</div>
      ${canAnswer ? `
        <div class="row" style="gap:8px; align-items:center">
          <input class="input" id="storyStickerAnswerInput" placeholder="اكتب إجابتك..." style="background:rgba(255,255,255,.10); color:#fff; border-color:rgba(255,255,255,.20)" />
          <button class="btn btn--primary" id="storyStickerAnswerBtn" type="button">إرسال</button>
        </div>
      ` : `<div class="storyStickerMeta">${answeredByMe ? 'تم إرسال إجابتك' : ''}</div>`}
      <div class="storyStickerMeta">${escapeHtml(String(answersCount))} إجابة</div>
      ${isOwner ? `<button class="btn" id="storyStickerViewAnswersBtn" type="button">عرض الإجابات</button>` : ''}
      ${answersHtml}
    `;

    const input = container.querySelector('#storyStickerAnswerInput');
    const sendBtn = container.querySelector('#storyStickerAnswerBtn');
    if (canAnswer && input instanceof HTMLInputElement && sendBtn instanceof HTMLButtonElement) {
      sendBtn.addEventListener('click', async () => {
        const text = String(input.value || '').trim();
        if (!text) return;
        sendBtn.disabled = true;
        try {
          const data = await apiFetch(`/stories/${encodeURIComponent(storyId)}/question/answer`, {
            method: 'POST',
            body: JSON.stringify({ text }),
          });
          s.interactive = data?.interactive || s.interactive;
          renderStoryInteractiveSticker(container, s);
        } catch (err) {
          sendBtn.disabled = false;
          setAlert(el('storyCommentsAlert'), `فشل الإرسال: ${err.data?.error || err.message}`, 'danger');
        }
      });
    }

    const viewBtn = container.querySelector('#storyStickerViewAnswersBtn');
    if (isOwner && viewBtn instanceof HTMLButtonElement) {
      viewBtn.addEventListener('click', async () => {
        viewBtn.disabled = true;
        try {
          const data = await apiFetch(`/stories/${encodeURIComponent(storyId)}/question/answers`);
          storyQuestionAnswersCache.set(storyId, data);
          renderStoryInteractiveSticker(container, s);
        } catch (err) {
          viewBtn.disabled = false;
          setAlert(el('storyCommentsAlert'), `فشل تحميل الإجابات: ${err.data?.error || err.message}`, 'danger');
        }
      });
    }

    return;
  }

  if (kind === 'link') {
    const title = String(inter.title || '').trim() || 'Link';
    const url = String(inter.url || '').trim();
    container.hidden = false;
    container.innerHTML = `
      <div class="storyStickerTitle">${escapeHtml(title)}</div>
      <div class="muted" style="color:rgba(255,255,255,.82)">${escapeHtml(url)}</div>
      <button class="btn btn--primary" id="storyStickerOpenLinkBtn" type="button">فتح الرابط</button>
    `;
    const btn = container.querySelector('#storyStickerOpenLinkBtn');
    if (btn instanceof HTMLButtonElement) {
      btn.addEventListener('click', () => {
        if (!url) return;
        try {
          window.open(url, '_blank', 'noopener');
        } catch {
          // ignore
        }
      });
    }
    return;
  }

  if (kind === 'mention') {
    const username = String(inter.username || '').trim().replace(/^@/, '');
    container.hidden = false;
    container.innerHTML = `
      <div class="storyStickerTitle">Mention</div>
      <div class="muted" style="color:rgba(255,255,255,.82)">@${escapeHtml(username)}</div>
      <button class="btn" id="storyStickerOpenMentionBtn" type="button">فتح البروفايل</button>
    `;
    const btn = container.querySelector('#storyStickerOpenMentionBtn');
    if (btn instanceof HTMLButtonElement) {
      btn.addEventListener('click', async () => {
        if (!requireToken()) return;
        if (!username) return;
        try {
          const data = await apiFetch(`/users?q=${encodeURIComponent(username)}`);
          const users = Array.isArray(data.users) ? data.users : [];
          const u = users.find((x) => String(x.username || '').trim().toLowerCase() === username.toLowerCase());
          if (u?.id) await openUserProfile(String(u.id));
        } catch {
          // ignore
        }
      });
    }
    return;
  }

  if (kind === 'location') {
    const name = String(inter.name || '').trim();
    container.hidden = false;
    container.innerHTML = `
      <div class="storyStickerTitle">Location</div>
      <div class="muted" style="color:rgba(255,255,255,.82)">${escapeHtml(name)}</div>
    `;
    return;
  }

  if (kind === 'countdown') {
    const title = String(inter.title || '').trim() || 'Countdown';
    const endAt = String(inter.endAt || '').trim();
    let remainingMs = 0;
    try {
      const ms = Date.parse(endAt);
      remainingMs = Number.isFinite(ms) ? Math.max(0, ms - Date.now()) : 0;
    } catch {
      remainingMs = 0;
    }
    const ended = remainingMs <= 0;
    container.hidden = false;
    container.innerHTML = `
      <div class="storyStickerTitle">${escapeHtml(title)}</div>
      <div class="storyStickerMeta">${ended ? 'انتهى' : `متبقي: <span id="storyStickerCountdownRemaining">${escapeHtml(formatDurationShort(remainingMs))}</span>`}</div>
    `;
    if (!ended && endAt) startStoryCountdownTicker(endAt);
    return;
  }

  if (kind === 'slider') {
    const emoji = String(inter.emoji || '❤️').trim() || '❤️';
    const canRespond = Boolean(inter.canRespond);
    const myValue = inter.myValue == null ? null : Number(inter.myValue);
    const avg = Number(inter.average || 0);
    const responsesCount = Number(inter.responsesCount || 0);
    const isOwner = Boolean(inter.isOwner);

    const cached = storyId ? storySliderResponsesCache.get(storyId) : null;
    const responsesHtml =
      isOwner && cached && Array.isArray(cached.responses)
        ? `
          <div class="divider" style="margin:10px 0; border-color:rgba(255,255,255,.14)"></div>
          <div class="storyStickerMeta">Responses: ${escapeHtml(String(cached.responses.length || 0))}</div>
          <div class="stack">
            ${cached.responses
              .slice(0, 60)
              .map((r) => {
                const u = r.user || { username: 'Unknown' };
                return `<div style="padding:8px 10px; border:1px solid rgba(255,255,255,.16); border-radius:12px; background:rgba(255,255,255,.06)"><strong>${escapeHtml(u.username || '')}${verifiedBadgeHtml(u.verified)}</strong><div>${escapeHtml(String(r.value ?? ''))}</div></div>`;
              })
              .join('')}
          </div>
        `
        : '';

    container.hidden = false;
    container.innerHTML = `
      <div class="storyStickerTitle">${escapeHtml(q)} ${escapeHtml(emoji)}</div>
      ${canRespond ? `
        <input id="storyStickerSliderRange" type="range" min="0" max="100" value="50" class="input" />
        <button class="btn btn--primary" id="storyStickerSliderSend" type="button">إرسال</button>
      ` : `<div class="storyStickerMeta">قيمتك: ${escapeHtml(String(myValue ?? ''))}</div>`}
      <div class="storyStickerMeta">متوسط: ${escapeHtml(String(avg))} • عدد: ${escapeHtml(String(responsesCount))}</div>
      ${isOwner ? `<button class="btn" id="storyStickerSliderViewBtn" type="button">عرض النتائج بالأسماء</button>` : ''}
      ${responsesHtml}
    `;

    const range = container.querySelector('#storyStickerSliderRange');
    const sendBtn = container.querySelector('#storyStickerSliderSend');
    if (canRespond && range instanceof HTMLInputElement && sendBtn instanceof HTMLButtonElement) {
      sendBtn.addEventListener('click', async (e) => {
        try {
          e.preventDefault();
        } catch {
          // ignore
        }
        try {
          e.stopPropagation();
        } catch {
          // ignore
        }

        if (!requireToken()) return;
        if (!storyId) {
          setAlert(el('storyCommentsAlert'), 'تعذر إرسال التفاعل: storyId غير موجود.', 'danger');
          return;
        }

        const value = Number(range.value || 0);
        if (!Number.isFinite(value)) return;

        const prevInteractive = s.interactive;
        try {
          const current = prevInteractive && typeof prevInteractive === 'object' ? prevInteractive : null;
          if (current) {
            const prevCount = Number(current.responsesCount || 0);
            const prevAvg = Number(current.average || 0);
            const nextCount = prevCount + 1;
            const nextAvg = nextCount ? Math.round((prevAvg * prevCount + value) / nextCount) : value;

            s.interactive = {
              ...current,
              canRespond: false,
              myValue: value,
              responsesCount: nextCount,
              average: nextAvg,
            };
            renderStoryInteractiveSticker(container, s);
          }
        } catch {
          // ignore
        }

        try {
          const data = await apiFetch(`/stories/${encodeURIComponent(storyId)}/slider/respond`, {
            method: 'POST',
            body: JSON.stringify({ value }),
            timeoutMs: 8000,
          });
          s.interactive = data?.interactive || s.interactive;
          renderStoryInteractiveSticker(container, s);
        } catch (err) {
          s.interactive = prevInteractive;
          renderStoryInteractiveSticker(container, s);
          setAlert(el('storyCommentsAlert'), `فشل الإرسال: ${err.data?.error || err.message}`, 'danger');
        }
      });
    }

    const viewBtn = container.querySelector('#storyStickerSliderViewBtn');
    if (isOwner && viewBtn instanceof HTMLButtonElement) {
      viewBtn.addEventListener('click', async () => {
        viewBtn.disabled = true;
        try {
          const data = await apiFetch(`/stories/${encodeURIComponent(storyId)}/slider/responses`);
          storySliderResponsesCache.set(storyId, data);
          renderStoryInteractiveSticker(container, s);
        } catch (err) {
          viewBtn.disabled = false;
          setAlert(el('storyCommentsAlert'), `فشل تحميل النتائج: ${err.data?.error || err.message}`, 'danger');
        }
      });
    }

    return;
  }

  if (kind === 'quiz') {
    const options = Array.isArray(inter.options) ? inter.options : [];
    const canAnswer = Boolean(inter.canAnswer);
    const showResults = Boolean(inter.showResults);
    const myAnswer = String(inter.myAnswerOptionId || '');
    const isOwner = Boolean(inter.isOwner);
    const correctOptionId = String(inter.correctOptionId || '');
    const isCorrect = inter.isCorrect == null ? null : Boolean(inter.isCorrect);

    const cached = storyId ? storyQuizAnswersCache.get(storyId) : null;
    const answersHtml =
      isOwner && cached && Array.isArray(cached.answers)
        ? `
          <div class="divider" style="margin:10px 0; border-color:rgba(255,255,255,.14)"></div>
          <div class="storyStickerMeta">Answers: ${escapeHtml(String(cached.answers.length || 0))}</div>
          <div class="stack">
            ${cached.answers
              .slice(0, 60)
              .map((a) => {
                const u = a.user || { username: 'Unknown' };
                const picked = String(a.optionId || '');
                const opt = (Array.isArray(cached.options) ? cached.options : []).find((o) => String(o.id) === picked);
                const txt = opt ? opt.text : '';
                const ok = cached.correctOptionId && picked && String(cached.correctOptionId) === picked;
                return `<div style="padding:8px 10px; border:1px solid rgba(255,255,255,.16); border-radius:12px; background:rgba(255,255,255,.06)"><strong>${escapeHtml(u.username || '')}${verifiedBadgeHtml(u.verified)}</strong><div>${escapeHtml(txt)}${ok ? ' ✓' : ''}</div></div>`;
              })
              .join('')}
          </div>
        `
        : '';

    container.hidden = false;
    container.innerHTML = `
      <div class="storyStickerTitle">${escapeHtml(q)}</div>
      <div class="stack" id="storyStickerQuizOpts"></div>
      ${showResults && correctOptionId ? `<div class="storyStickerMeta">${isCorrect == null ? '' : isCorrect ? 'إجابتك صح ✅' : 'إجابتك غلط ❌'}</div>` : ''}
      ${isOwner ? `<button class="btn" id="storyStickerQuizViewBtn" type="button">عرض الإجابات بالأسماء</button>` : ''}
      ${answersHtml}
    `;

    const list = container.querySelector('#storyStickerQuizOpts');
    if (list instanceof HTMLElement) {
      options.forEach((o) => {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'storyStickerOption';
        const oid = String(o?.id || '');
        const label = String(o?.text || '');
        const pct = Number(o?.pct || 0);
        const picked = myAnswer && oid && myAnswer === oid;
        const ok = showResults && correctOptionId && oid && correctOptionId === oid;
        btn.disabled = !canAnswer;
        btn.innerHTML = `
          <div style="text-align:right; flex:1">
            <div style="font-weight:800">${escapeHtml(label)}${picked ? ' ✓' : ''}${ok ? ' ✅' : ''}</div>
            ${showResults ? `<div class="storyStickerBar"><div class="storyStickerBarFill" style="width:${Math.max(0, Math.min(100, pct))}%"></div></div>` : ''}
          </div>
          ${showResults ? `<div style="min-width:64px; text-align:left; font-weight:900">${escapeHtml(String(pct))}%</div>` : ''}
        `;

        if (canAnswer) {
          btn.addEventListener('click', async () => {
            try {
              const data = await apiFetch(`/stories/${encodeURIComponent(storyId)}/quiz/answer`, {
                method: 'POST',
                body: JSON.stringify({ optionId: oid }),
              });
              s.interactive = data?.interactive || s.interactive;
              renderStoryInteractiveSticker(container, s);
            } catch (err) {
              setAlert(el('storyCommentsAlert'), `فشل الإجابة: ${err.data?.error || err.message}`, 'danger');
            }
          });
        }

        list.appendChild(btn);
      });
    }

    const viewBtn = container.querySelector('#storyStickerQuizViewBtn');
    if (isOwner && viewBtn instanceof HTMLButtonElement) {
      viewBtn.addEventListener('click', async () => {
        viewBtn.disabled = true;
        try {
          const data = await apiFetch(`/stories/${encodeURIComponent(storyId)}/quiz/answers`);
          storyQuizAnswersCache.set(storyId, data);
          renderStoryInteractiveSticker(container, s);
        } catch (err) {
          viewBtn.disabled = false;
          setAlert(el('storyCommentsAlert'), `فشل تحميل الإجابات: ${err.data?.error || err.message}`, 'danger');
        }
      });
    }

    return;
  }

  container.hidden = true;
  container.innerHTML = '';
}

function stopReelsAutoplay() {
  try {
    if (reelsObserver) {
      reelsObserver.disconnect();
      reelsObserver = null;
    }
  } catch {
    // ignore
  }
}

function stopSoundsAutoplay() {
  try {
    if (soundObserver) {
      soundObserver.disconnect();
      soundObserver = null;
    }
  } catch {
    // ignore
  }

  try {
    document.querySelectorAll('audio.audioPlayer[data-autosound="1"]').forEach((a) => {
      try {
        a.pause();
        a.currentTime = 0;
      } catch {
        // ignore
      }
    });
  } catch {
    // ignore
  }
}

function stopAllSoundsExcept(exceptEl) {
  try {
    document.querySelectorAll('audio.audioPlayer[data-autosound="1"]').forEach((a) => {
      if (exceptEl && a === exceptEl) return;
      try {
        a.pause();
        a.currentTime = 0;
      } catch {
        // ignore
      }
    });
  } catch {
    // ignore
  }
}

function stopStorySound() {
  if (!storySoundEl) return;
  try {
    storySoundEl.pause();
    storySoundEl.currentTime = 0;
  } catch {
    // ignore
  }
  storySoundEl = null;
}

async function playStorySound(url) {
  stopStorySound();
  const u = String(url || '').trim();
  if (!u) return;
  try {
    const a = new Audio(u);
    a.preload = 'metadata';
    storySoundEl = a;
    await tryPlaySound(a);
  } catch {
    // ignore
  }
}

async function tryPlaySound(audioEl) {
  if (!audioEl) return;

  try {
    audioEl.loop = true;
    audioEl.playsInline = true;
  } catch {
    // ignore
  }

  stopAllSoundsExcept(audioEl);

  if (!audioAutoplayUnlocked) {
    try {
      audioEl.muted = true;
      await audioEl.play();
      return;
    } catch {
      return;
    }
  }

  try {
    audioEl.muted = false;
    await audioEl.play();
  } catch {
    try {
      audioEl.muted = true;
      await audioEl.play();
    } catch {
      // ignore
    }
  }
}

function setupSoundsAutoplay(rootEl) {
  stopSoundsAutoplay();

  const root = rootEl || document;
  const wraps = Array.from(root.querySelectorAll('.post'));
  const candidates = wraps.filter((w) => w.querySelector('audio.audioPlayer[data-autosound="1"]'));
  if (!candidates.length) return;

  soundObserver = new IntersectionObserver(
    (entries) => {
      entries.forEach((e) => {
        const wrap = e.target;
        if (!(wrap instanceof HTMLElement)) return;
        const a = wrap.querySelector('audio.audioPlayer[data-autosound="1"]');
        if (!(a instanceof HTMLAudioElement)) return;
        if (e.isIntersecting && e.intersectionRatio >= 0.6) {
          tryPlaySound(a);
        } else {
          try {
            a.pause();
            a.currentTime = 0;
          } catch {
            // ignore
          }
        }
      });
    },
    { threshold: [0, 0.25, 0.6, 1] }
  );

  candidates.forEach((w) => soundObserver.observe(w));
}

function setupAudioAutoplayUnlock() {
  if (audioAutoplayUnlocked) return;

  const unlock = async () => {
    if (audioAutoplayUnlocked) return;
    audioAutoplayUnlocked = true;

    // Try to re-play the currently most visible sound (now unmuted if allowed).
    try {
      const wraps = Array.from(document.querySelectorAll('.post'));
      const visibleWrap = wraps.find((w) => {
        try {
          const r = w.getBoundingClientRect();
          const vh = window.innerHeight || document.documentElement.clientHeight || 0;
          const vw = window.innerWidth || document.documentElement.clientWidth || 0;
          const inView = r.width > 0 && r.height > 0 && r.top < vh * 0.5 && r.bottom > vh * 0.3 && r.left < vw && r.right > 0;
          return inView;
        } catch {
          return false;
        }
      });
      const a = visibleWrap?.querySelector('audio.audioPlayer[data-autosound="1"]');
      if (a instanceof HTMLAudioElement) await tryPlaySound(a);
    } catch {
      // ignore
    }
  };

  const opts = { once: true, passive: true };
  window.addEventListener('pointerdown', unlock, opts);
  window.addEventListener('touchstart', unlock, opts);
  window.addEventListener('keydown', unlock, opts);
  window.addEventListener('scroll', unlock, opts);
}

async function refreshSounds() {
  if (!requireToken()) return;
  const data = await apiFetch('/sounds');
  state.sounds = Array.isArray(data.sounds) ? data.sounds : [];
}

function renderSoundPicker() {
  const wrap = el('soundPickerWrap');
  const select = el('soundSelect');
  const preview = el('soundPreview');
  const hint = el('soundHint');
  const addWrap = el('addSoundWrap');
  if (!wrap || !select || !preview || !hint) return;

  wrap.hidden = false;
  if (addWrap) addWrap.hidden = !Boolean(state.me?.verified);

  const sounds = Array.isArray(state.sounds) ? state.sounds : [];
  select.innerHTML = '';

  const optNone = document.createElement('option');
  optNone.value = '';
  optNone.textContent = 'بدون صوت';
  select.appendChild(optNone);

  sounds.forEach((s) => {
    const o = document.createElement('option');
    o.value = String(s.id || '');
    o.textContent = String(s.title || 'Sound');
    select.appendChild(o);
  });

  const hasSelected = sounds.some((s) => String(s.id) === String(state.selectedSoundId));
  if (!hasSelected) state.selectedSoundId = '';
  select.value = state.selectedSoundId;

  hint.hidden = sounds.length > 0;
  hint.textContent = sounds.length > 0 ? '' : 'لا يوجد أصوات متاحة حاليًا.';

  const selected = sounds.find((s) => String(s.id) === String(select.value));
  if (selected?.url) {
    preview.hidden = false;
    preview.src = String(selected.url);
  } else {
    preview.hidden = true;
    preview.src = '';
  }
}

async function refreshCreateSoundsUI() {
  if (!requireToken()) return;
  if (!state.me) await loadMe();
  try {
    await refreshSounds();
  } catch {
    state.sounds = [];
  }
  renderSoundPicker();
  el('soundPickerWrap') && (el('soundPickerWrap').hidden = false);
}

function setupReelsAutoplay() {
  stopReelsAutoplay();

  const videos = Array.from(document.querySelectorAll('#reelsList .reelVideo'));
  if (!videos.length) return;

  videos.forEach((v) => {
    v.muted = true;
    v.playsInline = true;
    v.loop = true;
    v.preload = 'metadata';

    v.addEventListener('click', () => {
      try {
        if (v.paused) {
          void v.play();
        } else {
          v.pause();
        }
      } catch {
        // ignore
      }
    });

    v.addEventListener('dblclick', () => {
      v.muted = !v.muted;
    });
  });

  reelsObserver = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        const v = entry.target;
        if (!(v instanceof HTMLVideoElement)) return;

        if (entry.isIntersecting && entry.intersectionRatio >= 0.75) {
          videos.forEach((other) => {
            if (other !== v) {
              try {
                other.pause();
              } catch {
                // ignore
              }
            }
          });

          try {
            void v.play();
          } catch {
            // ignore
          }
        } else {
          try {
            v.pause();
          } catch {
            // ignore
          }
        }
      });
    },
    { threshold: [0, 0.25, 0.5, 0.75, 1] },
  );

  videos.forEach((v) => reelsObserver.observe(v));
}

function setChatPolling(active) {
  if (chatPollTimer) {
    clearInterval(chatPollTimer);
    chatPollTimer = null;
  }
  if (!active) return;

  chatPollTimer = window.setInterval(async () => {
    try {
      if (state.activeView !== 'chats') return;
      if (!state.selectedChatId) return;

      const chatId = state.selectedChatId;
      const data = await apiFetch(`/chats/${chatId}/messages`);
      const lastId = data.messages?.length ? String(data.messages[data.messages.length - 1].id) : '';
      const key = `${chatId}:${lastId}`;
      if (key === lastChatPollKey) return;
      lastChatPollKey = key;

      const title = data.chat?.displayTitle || el('chatTitle').textContent;
      el('chatTitle').textContent = title;
      renderMessages(data.messages || []);
      await refreshChats();
      await refreshNotifications();
    } catch {
      // ignore polling errors
    }
  }, 1500);
}

function stopAppPolling() {
  if (appPollTimer) {
    clearInterval(appPollTimer);
    appPollTimer = null;
  }
  appPollTick = 0;
}

function startAppPolling() {
  stopAppPolling();
  if (!state.token) return;

  appPollTimer = window.setInterval(async () => {
    if (!state.token) return;
    if (state.activeView === 'auth') return;

    appPollTick += 1;
    try {
      await refreshNotifications();
    } catch {
      // ignore
    }

    try {
      if (state.activeView === 'feed') {
        if (appPollTick % 3 === 0) {
          await refreshFeed();
        } else {
          await refreshStories();
          renderStoriesBar();
        }
      }

      if (state.activeView === 'notifications') {
        if (appPollTick % 2 === 0) await refreshNotifications();
      }
    } catch {
      // ignore
    }
  }, 4000);
}

function setAlert(target, message, type) {
  if (!target) return;
  if (!message) {
    target.hidden = true;
    target.textContent = '';
    target.className = 'alert';
    return;
  }
  target.hidden = false;
  target.textContent = message;
  target.className = `alert ${type === 'ok' ? 'alert--ok' : type === 'danger' ? 'alert--danger' : ''}`;
}

function renderChatSearchUsers(users) {
  const list = el('chatUserSearchList');
  list.innerHTML = '';

  if (!users.length) {
    list.innerHTML = '<div class="muted">لا يوجد نتائج.</div>';
    return;
  }

  users.forEach((u) => {
    if (u.id === state.me?.id) return;
    const div = document.createElement('div');
    div.className = 'item';
    div.innerHTML = `
      <div class="item__top">
        <div>
          <div class="item__title">${escapeHtml(u.username)}</div>
          <div class="item__sub"><span class="muted">${escapeHtml(u.id)}</span></div>
        </div>
        <button class="btn" data-chat="${u.id}">شات</button>
      </div>
    `;

    div.querySelector('[data-chat]')?.addEventListener('click', async () => {
      const data = await apiFetch('/chats/direct', {
        method: 'POST',
        body: JSON.stringify({ otherUserId: u.id }),
      });

      const chat = data.chat;
      state.selectedChatId = chat.id;
      await refreshChats();
      await openChat(chat.id);
    });

    list.appendChild(div);
  });
}

function readFileAsDataUrl(file) {
  return new Promise((resolve) => {
    const reader = new FileReader();
    reader.onerror = () => resolve(null);
    reader.onload = () => {
      const url = String(reader.result || '');
      const type = String(file.type || '');
      resolve({
        type,
        url,
        name: file.name,
        size: file.size,
      });
    };
    reader.readAsDataURL(file);
  });
}

function renderMediaPreview() {
  const box = el('postMediaPreview');
  box.innerHTML = '';
  if (!state.postMediaDraft.length) {
    box.hidden = true;
    return;
  }
  box.hidden = false;

  state.postMediaDraft.forEach((m) => {
    const type = String(m.type || '');
    const url = String(m.url || '');
    if (!url) return;
    const div = document.createElement('div');
    div.className = 'mediaItem';
    div.innerHTML = type.startsWith('video')
      ? `<video class="mediaThumb" src="${url}" controls></video>`
      : `<img class="mediaThumb" alt="" src="${url}" />`;
    box.appendChild(div);
  });
}

function renderNotifications(notifications) {
  const notifAlert = el('notificationsAlert');
  setAlert(notifAlert, '', '');
  const list = el('notificationsList');
  list.innerHTML = '';

  if (!notifications.length) {
    list.innerHTML = '<div class="muted">لا يوجد إشعارات.</div>';
    return;
  }

  notifications.forEach((n) => {
    const div = document.createElement('div');
    div.className = 'item';
    const title =
      n.type === 'message'
        ? 'رسالة'
        : n.type === 'admin'
          ? 'تنبيه'
        : n.type === 'like'
          ? 'لايك'
          : n.type === 'follow'
            ? 'متابعة'
            : 'إشعار';

    div.innerHTML = `
      <div class="item__top">
        <div>
          <div class="item__title">${escapeHtml(title)}${n.read ? '' : ' • جديد'}</div>
          <div class="item__sub">${escapeHtml(n.message || '')} • ${escapeHtml(formatTime(n.createdAt))}</div>
        </div>
        <div class="row row--wrap">
          <button class="btn" data-open="${escapeHtml(n.type)}">فتح</button>
          <button class="btn btn--ghost" data-read>قراءة</button>
        </div>
      </div>
    `;

    div.querySelector('[data-read]')?.addEventListener('click', async () => {
      await apiFetch(`/notifications/${encodeURIComponent(n.id)}/read`, { method: 'POST' });
      await refreshNotifications();
    });

    div.querySelector('[data-open]')?.addEventListener('click', async () => {
      setAlert(notifAlert, '', '');

      try {
        if (!n.read) {
          await apiFetch(`/notifications/${encodeURIComponent(n.id)}/read`, { method: 'POST' });
        }

        if (n.type === 'message' && n.chatId) {
          showView('chats');
          await refreshChats();
          state.selectedChatId = n.chatId;
          await openChat(n.chatId);
          await refreshNotifications();
          return;
        }

        if ((n.type === 'follow' || n.type === 'like') && n.actorId) {
          await loadMe();
          await openUserProfile(n.actorId);
          await refreshNotifications();
          return;
        }

        if (n.type === 'admin' && n.actorId) {
          window.alert(String(n.message || ''));
          await refreshNotifications();
          return;
        }

        await refreshNotifications();
      } catch (err) {
        console.error('Open notification failed:', err);
        setAlert(notifAlert, `فشل فتح الإشعار: ${err.data?.error || err.message}`, 'danger');
      }
    });

    list.appendChild(div);
  });
}

function formatTime(iso) {
  try {
    const d = new Date(iso);
    return d.toLocaleString('ar-EG');
  } catch {
    return iso;
  }
}

function avatarFallback(name) {
  const safe = encodeURIComponent(String(name || 'N').slice(0, 1).toUpperCase());
  return `https://api.dicebear.com/7.x/thumbs/svg?seed=${safe}`;
}

async function apiFetch(path, options = {}) {
  const { timeoutMs, ...fetchOptions } = options || {};
  const headers = { 'Content-Type': 'application/json', ...(fetchOptions.headers || {}) };
  if (state.token) headers.Authorization = `Bearer ${state.token}`;

  const useExternalSignal = Boolean(fetchOptions.signal);
  const controller = !useExternalSignal && timeoutMs ? new AbortController() : null;
  const signal = useExternalSignal ? fetchOptions.signal : controller ? controller.signal : undefined;
  const timer =
    controller && timeoutMs
      ? window.setTimeout(() => {
          try {
            controller.abort();
          } catch {
            // ignore
          }
        }, Math.max(250, Number(timeoutMs) || 0))
      : null;

  let res;
  let text = '';
  try {
    res = await fetch(`${API}${path}`, { ...fetchOptions, headers, ...(signal ? { signal } : {}) });
    text = await res.text();
  } catch (err) {
    if (timer) clearTimeout(timer);
    if (err && err.name === 'AbortError') {
      const e = new Error('timeout');
      e.status = 0;
      e.data = { error: 'timeout' };
      throw e;
    }
    throw err;
  }
  if (timer) clearTimeout(timer);
  let data;
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = { raw: text };
  }

  if (!res.ok) {
    const err = new Error(data?.error || 'request_failed');
    err.status = res.status;
    err.data = data;
    throw err;
  }

  return data;
}

function setLoggedInUI(loggedIn) {
  el('sidebar').hidden = !loggedIn;
  el('logoutBtn').hidden = !loggedIn;
  el('userChip').hidden = !loggedIn;
  el('notifBtn').hidden = !loggedIn;
  el('adminNavItem') && (el('adminNavItem').hidden = true);

  if (!loggedIn) {
    el('userChipAvatar').src = '';
    el('userChipName').textContent = '';
    el('userChipSub').textContent = '';
    el('notifBadge').hidden = true;
    el('notifBadge').textContent = '';
  }
}

function showView(name) {
  if (state.activeView === 'reels' && name !== 'reels') {
    stopReelsAutoplay();
  }
  if (state.activeView === 'meetings' && name !== 'meetings') {
    try {
      stopMeeting();
    } catch {
      // ignore
    }
  }
  if (state.activeView && state.activeView !== name) {
    stopSoundsAutoplay();
  }
  if (name !== 'feed') {
    try {
      closeStoryViewer();
    } catch {
      // ignore
    }
  }
  if (name !== 'highlight') {
    try {
      closeHighlightViewer();
    } catch {
      // ignore
    }
  }
  state.activeView = name;

  const views = {
    auth: el('view-auth'),
    meetings: el('view-meetings'),
    reels: el('view-reels'),
    feed: el('view-feed'),
    create: el('view-create'),
    saved: el('view-saved'),
    chats: el('view-chats'),
    notifications: el('view-notifications'),
    profile: el('view-profile'),
    user: el('view-user'),
    dashboard: el('view-dashboard'),
  };

  Object.entries(views).forEach(([k, node]) => {
    node.hidden = k !== name;
  });

  document.querySelectorAll('.nav__item').forEach((btn) => {
    btn.classList.toggle('nav__item--active', btn.dataset.view === name);
  });
}

async function refreshStories() {
  if (!requireToken()) return;
  try {
    const data = await apiFetch('/stories');
    state.stories = Array.isArray(data.stories) ? data.stories : [];
  } catch {
    state.stories = [];
  }
}

function groupStories(stories) {
  const map = new Map();
  (Array.isArray(stories) ? stories : []).forEach((s) => {
    const author = s.author || null;
    const uid = String(author?.id || s.userId || '');
    if (!uid) return;
    if (!map.has(uid)) {
      map.set(uid, { userId: uid, author, stories: [] });
    }
    map.get(uid).stories.push(s);
  });

  const arr = Array.from(map.values());
  arr.forEach((g) => {
    g.stories = g.stories
      .slice()
      .sort((a, b) => (a.createdAt < b.createdAt ? -1 : 1));
  });

  arr.sort((a, b) => {
    const aHasUnseen = a.stories.some((x) => !x.seenByMe);
    const bHasUnseen = b.stories.some((x) => !x.seenByMe);
    if (aHasUnseen !== bHasUnseen) return aHasUnseen ? -1 : 1;
    const aLast = a.stories[a.stories.length - 1];
    const bLast = b.stories[b.stories.length - 1];
    return aLast?.createdAt < bLast?.createdAt ? 1 : -1;
  });

  return arr;
}

function renderStoriesBar() {
  const bar = el('storiesBar');
  const list = el('storiesList');
  const createBtn = el('createStoryFromFeedBtn');
  const createAvatar = el('createStoryFromFeedAvatar');
  if (!bar || !list || !createBtn) return;

  if (createAvatar) {
    const src = state.me?.avatarUrl || avatarFallback(state.me?.username || '');
    createAvatar.src = src;
  }

  const groups = groupStories(state.stories || []);
  bar.hidden = false;
  list.innerHTML = '';

  if (!groups.length) {
    return;
  }

  groups.forEach((g, idx) => {
    const author = g.author || { username: 'Unknown', avatarUrl: '' };
    const seen = g.stories.length > 0 && g.stories.every((x) => x.seenByMe);
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = `storyBubble ${seen ? 'storyBubble--seen' : ''}`;
    btn.innerHTML = `
      <div class="storyBubble__ring">
        <img class="storyBubble__avatar" alt="" src="${author.avatarUrl || avatarFallback(author.username)}" />
      </div>
      <div class="storyBubble__name">${escapeHtml(author.username || '')}</div>
    `;
    btn.addEventListener('click', () => {
      openStoryViewer(groups, idx, 0);
    });
    list.appendChild(btn);
  });
}

function stopStoryTimer() {
  if (storyViewerState.timerId) {
    clearTimeout(storyViewerState.timerId);
    storyViewerState.timerId = null;
  }
  if (storyViewerState.rafId) {
    cancelAnimationFrame(storyViewerState.rafId);
    storyViewerState.rafId = 0;
  }
}

function stopStoryCountdownTicker() {
  if (storyViewerState.countdownTimerId) {
    clearInterval(storyViewerState.countdownTimerId);
    storyViewerState.countdownTimerId = null;
  }
  storyViewerState.countdownEndAt = '';
}

function startStoryCountdownTicker(endAt) {
  stopStoryCountdownTicker();
  const endMs = Date.parse(String(endAt || ''));
  if (!Number.isFinite(endMs)) return;
  storyViewerState.countdownEndAt = String(endAt || '');

  const tick = () => {
    if (!storyViewerState.open) return;
    const span = el('storyStickerCountdownRemaining');
    if (!(span instanceof HTMLElement)) return;
    const remainingMs = Math.max(0, endMs - Date.now());
    span.textContent = remainingMs <= 0 ? 'انتهى' : formatDurationShort(remainingMs);
  };

  tick();
  storyViewerState.countdownTimerId = window.setInterval(tick, 1000);
}

function closeStoryViewer() {
  const modal = el('storyModal');
  if (modal) modal.hidden = true;
  stopStoryTimer();
  stopStoryCountdownTicker();
  stopStorySound();
  storyViewerState.open = false;

  try {
    const body = el('storyViewerBody');
    body?.querySelectorAll('video').forEach((v) => {
      try {
        v.pause();
      } catch {
        // ignore
      }
    });
  } catch {
    // ignore
  }
}

function renderStoryProgress(group) {
  const box = el('storyProgress');
  if (!box) return;
  box.innerHTML = '';
  const count = group?.stories?.length || 0;
  for (let i = 0; i < count; i++) {
    const seg = document.createElement('div');
    seg.className = 'storyProgress__seg';
    const fill = document.createElement('div');
    fill.className = 'storyProgress__fill';
    fill.style.width = i < storyViewerState.storyIndex ? '100%' : '0%';
    seg.appendChild(fill);
    box.appendChild(seg);
  }
}

function updateStoryProgressFrame() {
  const box = el('storyProgress');
  if (!box) return;
  const seg = box.children[storyViewerState.storyIndex];
  const fill = seg?.querySelector('.storyProgress__fill');
  if (!(fill instanceof HTMLElement)) return;

  const now = Date.now();
  const elapsed = Math.max(0, now - storyViewerState.startedAt);
  const pct = storyViewerState.durationMs ? Math.min(1, elapsed / storyViewerState.durationMs) : 0;
  fill.style.width = `${Math.round(pct * 100)}%`;

  if (pct >= 1) return;
  storyViewerState.rafId = requestAnimationFrame(updateStoryProgressFrame);
}

async function markStoryViewed(storyId) {
  if (!requireToken()) return;
  if (!storyId) return;
  try {
    await apiFetch(`/stories/${encodeURIComponent(String(storyId))}/view`, { method: 'POST' });
  } catch {
    // ignore
  }
}

function openStoryViewer(groups, groupIndex, storyIndex) {
  const modal = el('storyModal');
  const body = el('storyViewerBody');
  const authorBox = el('storyViewerAuthor');
  if (!modal || !body) return;

  storyViewerState.open = true;
  storyViewerState.groups = Array.isArray(groups) ? groups : [];
  storyViewerState.groupIndex = Math.max(0, Math.min(groupIndex || 0, storyViewerState.groups.length - 1));
  const group = storyViewerState.groups[storyViewerState.groupIndex];
  const count = group?.stories?.length || 0;
  storyViewerState.storyIndex = Math.max(0, Math.min(storyIndex || 0, Math.max(0, count - 1)));

  modal.hidden = false;
  stopStoryTimer();
  stopStoryCountdownTicker();
  stopStorySound();
  renderStoryProgress(group);

  const author = group?.author || { username: 'Unknown', avatarUrl: '' };
  if (authorBox) {
    authorBox.innerHTML = `
      <img alt="" src="${author.avatarUrl || avatarFallback(author.username)}" />
      <div>${escapeHtml(author.username || '')}</div>
    `;
  }

  const s = group?.stories?.[storyViewerState.storyIndex];
  if (!s) {
    closeStoryViewer();
    return;
  }

  if (s?.sound?.url) {
    void playStorySound(s.sound.url);
  }

  const saveBtn = el('saveStoryToHighlightBtn');
  if (saveBtn) {
    const isMine = String(s.userId || group?.userId || '') && String(s.userId || group?.userId || '') === String(state.me?.id || '');
    saveBtn.hidden = !isMine;
  }

  setAlert(el('storyCommentsAlert'), '', '');
  if (el('storyCommentInput')) el('storyCommentInput').value = '';
  void refreshStoryComments();

  const rendered = renderStoryCanvas(body, s);
  try {
    const inter = s.interactive && typeof s.interactive === 'object' ? s.interactive : null;
    if (inter && String(inter.kind || '').trim().toLowerCase() === 'countdown' && inter.endAt) {
      startStoryCountdownTicker(inter.endAt);
    }
  } catch {
    // ignore
  }
  if (rendered?.kind === 'video' && rendered.el) {
    const v = rendered.el;
    v.addEventListener('loadedmetadata', () => {
      const secs = Number(v.duration || 0);
      const ms = Number.isFinite(secs) && secs > 0 ? Math.min(15000, Math.max(2500, secs * 1000)) : 7000;
      startStoryTimer(ms);
      try {
        void v.play();
      } catch {
        // ignore
      }
    }, { once: true });
  } else {
    startStoryTimer(5000);
  }

  void markStoryViewed(s.id);
}

function startStoryTimer(durationMs) {
  stopStoryTimer();
  storyViewerState.durationMs = Math.max(500, Number(durationMs || 0));
  storyViewerState.startedAt = Date.now();
  storyViewerState.rafId = requestAnimationFrame(updateStoryProgressFrame);
  storyViewerState.timerId = setTimeout(() => {
    try {
      goToNextStory();
    } catch {
      // ignore
    }
  }, storyViewerState.durationMs);
}

function goToNextStory() {
  const groups = storyViewerState.groups || [];
  const g = groups[storyViewerState.groupIndex];
  const count = g?.stories?.length || 0;
  const nextStoryIndex = storyViewerState.storyIndex + 1;
  if (nextStoryIndex < count) {
    openStoryViewer(groups, storyViewerState.groupIndex, nextStoryIndex);
    return;
  }

  const nextGroupIndex = storyViewerState.groupIndex + 1;
  if (nextGroupIndex < groups.length) {
    openStoryViewer(groups, nextGroupIndex, 0);
    return;
  }

  closeStoryViewer();
}

function goToPrevStory() {
  const groups = storyViewerState.groups || [];
  const g = groups[storyViewerState.groupIndex];
  const count = g?.stories?.length || 0;
  const prevStoryIndex = storyViewerState.storyIndex - 1;
  if (prevStoryIndex >= 0) {
    openStoryViewer(groups, storyViewerState.groupIndex, prevStoryIndex);
    return;
  }

  const prevGroupIndex = storyViewerState.groupIndex - 1;
  if (prevGroupIndex >= 0) {
    const prevGroup = groups[prevGroupIndex];
    const lastIdx = Math.max(0, (prevGroup?.stories?.length || 1) - 1);
    openStoryViewer(groups, prevGroupIndex, lastIdx);
    return;
  }

  if (count > 0) openStoryViewer(groups, storyViewerState.groupIndex, 0);
}

async function refreshAdminStatus() {
  state.isAdmin = false;
  if (el('adminNavItem')) el('adminNavItem').hidden = true;

  if (!state.token) return;

  try {
    const data = await apiFetch('/admin/status');
    state.isAdmin = Boolean(data.isAdmin);
    if (el('adminNavItem')) el('adminNavItem').hidden = !state.isAdmin;
  } catch {
    state.isAdmin = false;
    if (el('adminNavItem')) el('adminNavItem').hidden = true;
  }
}

function renderDashboardOverview() {
  const list = el('dashboardOverview');
  if (!list) return;
  list.innerHTML = '';

  const overview = state.adminOverview;
  if (!overview) {
    list.innerHTML = '<div class="muted">لا يوجد بيانات.</div>';
    return;
  }

  const totals = overview.totals || {};
  const lastWindow = overview.lastWindow || {};
  const rows = [
    { k: 'إجمالي المستخدمين', v: totals.users },
    { k: 'إجمالي البوستات', v: totals.posts },
    { k: 'إجمالي الريلز', v: totals.reels },
    { k: 'إجمالي المحادثات', v: totals.chats },
    { k: 'إجمالي الرسائل', v: totals.messages },
    { k: `بوستات آخر ${overview.lastDays} يوم`, v: lastWindow.posts },
    { k: `ريلز آخر ${overview.lastDays} يوم`, v: lastWindow.reels },
  ];

  rows.forEach((r) => {
    const div = document.createElement('div');
    div.className = 'item';
    div.innerHTML = `
      <div class="item__top">
        <div>
          <div class="item__title">${escapeHtml(r.k)}</div>
          <div class="item__sub">${escapeHtml(String(r.v ?? 0))}</div>
        </div>
      </div>
    `;
    list.appendChild(div);
  });
}

function renderAdminUsersStats() {
  const list = el('adminUsersStats');
  if (!list) return;
  list.innerHTML = '';

  const q = String(el('adminUserSearchInput')?.value || '').trim().toLowerCase();
  const stats = Array.isArray(state.adminUsersStats) ? state.adminUsersStats : [];
  const filtered = q
    ? stats.filter((s) => {
        const username = String(s.username || '').toLowerCase();
        const email = String(s.email || '').toLowerCase();
        const id = String(s.id || '').toLowerCase();
        return username.includes(q) || email.includes(q) || id.includes(q);
      })
    : stats;

  if (!filtered.length) {
    list.innerHTML = '<div class="muted">لا يوجد نتائج.</div>';
    return;
  }

  filtered.slice(0, 40).forEach((s) => {
    const div = document.createElement('div');
    div.className = 'item';
    const score = Number(s.likesReceived || 0) + Number(s.savesReceived || 0) + Number(s.commentsReceived || 0) * 2;
    div.innerHTML = `
      <div class="item__top">
        <div>
          <div class="item__title">${escapeHtml(s.username || '')}${verifiedBadgeHtml(s.verified)}</div>
          <div class="item__sub"><span class="muted">${escapeHtml(s.email || '')}</span> • ${escapeHtml(String(
            s.id || ''
          ))}</div>
        </div>
        <div class="row row--wrap">
          <button class="btn" data-admin-chat="${escapeHtml(String(s.id || ''))}">شات</button>
          <button class="btn" data-admin-notify="${escapeHtml(String(s.id || ''))}">إنذار</button>
          <button class="btn" data-admin-verify="${escapeHtml(String(s.id || ''))}">${s.verified ? 'إلغاء توثيق' : 'توثيق'}</button>
          <button class="btn btn--ghost" data-admin-deluser="${escapeHtml(String(s.id || ''))}">حذف</button>
        </div>
      </div>
      <div class="item__sub">بوستات: ${escapeHtml(String(s.postsCount || 0))} • ريلز: ${escapeHtml(
        String(s.reelsCount || 0)
      )} • تفاعل: ${escapeHtml(String(score))}</div>
    `;

    div.querySelector('[data-admin-notify]')?.addEventListener('click', () => {
      el('adminNotifyUserId').value = String(s.id || '');
      el('adminNotifyTitle').value = 'تنبيه';
      el('adminNotifyMessage').value = '';
      setAlert(el('adminNotifyAlert'), '', '');
    });

    div.querySelector('[data-admin-chat]')?.addEventListener('click', async () => {
      try {
        const data = await apiFetch('/chats/direct', {
          method: 'POST',
          body: JSON.stringify({ otherUserId: String(s.id || '') }),
        });
        state.selectedChatId = data.chat.id;
        showView('chats');
        await refreshChats();
        await openChat(data.chat.id);
      } catch {
        // ignore
      }
    });

    div.querySelector('[data-admin-deluser]')?.addEventListener('click', async () => {
      const uid = String(s.id || '');
      if (!uid) return;
      if (!window.confirm('تأكيد حذف هذا الحساب؟ سيتم حذف بياناته.')) return;
      await apiFetch(`/admin/users/${encodeURIComponent(uid)}`, { method: 'DELETE' });
      await refreshDashboard();
    });

    div.querySelector('[data-admin-verify]')?.addEventListener('click', async () => {
      const uid = String(s.id || '');
      if (!uid) return;
      await apiFetch(`/admin/users/${encodeURIComponent(uid)}/verify`, {
        method: 'POST',
        body: JSON.stringify({ verified: !Boolean(s.verified) }),
      });
      await refreshDashboard();
    });

    list.appendChild(div);
  });
}

function renderAdminRecentPosts() {
  const list = el('adminRecentPosts');
  if (!list) return;
  list.innerHTML = '';

  const items = Array.isArray(state.adminRecentPosts) ? state.adminRecentPosts : [];
  const filtered =
    state.adminPostsFilter === 'post'
      ? items.filter((p) => String(p.kind || 'post') === 'post')
      : state.adminPostsFilter === 'reel'
        ? items.filter((p) => String(p.kind || 'post') === 'reel')
        : items;

  if (!filtered.length) {
    list.innerHTML = '<div class="muted">لا يوجد نتائج.</div>';
    return;
  }

  filtered.slice(0, 50).forEach((p) => {
    const div = document.createElement('div');
    div.className = 'item';
    div.innerHTML = `
      <div class="item__top">
        <div>
          <div class="item__title">${escapeHtml(p.kind === 'reel' ? 'ريل' : 'بوست')} • ${escapeHtml(
            p.author?.username || 'Unknown'
          )}</div>
          <div class="item__sub">${escapeHtml(formatTime(p.createdAt))} • ❤️ ${escapeHtml(
            String(p.likesCount || 0)
          )} • 💾 ${escapeHtml(String(p.savesCount || 0))} • 💬 ${escapeHtml(String(p.commentsCount || 0))}</div>
        </div>
        <div class="row row--wrap">
          <button class="btn" data-admin-openuser="${escapeHtml(String(p.userId || ''))}">حساب</button>
          <button class="btn btn--ghost" data-admin-delpost="${escapeHtml(String(p.id || ''))}">حذف</button>
        </div>
      </div>
      ${p.text ? `<div class="item__sub">${escapeHtml(String(p.text || '')).slice(0, 160)}</div>` : ''}
    `;

    div.querySelector('[data-admin-openuser]')?.addEventListener('click', async () => {
      const uid = String(p.userId || '');
      if (!uid) return;
      await openUserProfile(uid);
    });

    div.querySelector('[data-admin-delpost]')?.addEventListener('click', async () => {
      const pid = String(p.id || '');
      if (!pid) return;
      if (!window.confirm('تأكيد حذف هذا المنشور/الريل؟')) return;
      await apiFetch(`/admin/posts/${encodeURIComponent(pid)}`, { method: 'DELETE' });
      await refreshDashboard();
    });

    list.appendChild(div);
  });
}

async function refreshDashboard() {
  if (!requireToken()) return;
  await refreshAdminStatus();

  const hint = el('dashboardAccessHint');
  if (hint) {
    hint.hidden = true;
    hint.textContent = '';
  }

  if (!state.isAdmin) {
    if (hint) {
      hint.hidden = false;
      hint.textContent = 'هذه الصفحة للأدمن فقط. تأكد من ضبط ADMIN_EMAILS (أو ADMIN_EMAIL) في ملف .env.';
    }
    showView('feed');
    if (window.location.pathname === '/dashbord') window.history.replaceState({}, '', '/');
    return;
  }

  try {
    state.adminOverview = await apiFetch('/admin/overview?days=30');
    const stats = await apiFetch('/admin/users/stats');
    state.adminUsersStats = stats.stats || [];
    const posts = await apiFetch('/admin/posts?days=30');
    state.adminRecentPosts = posts.posts || [];
    const sounds = await apiFetch('/sounds');
    state.adminSounds = Array.isArray(sounds.sounds) ? sounds.sounds : [];

    renderDashboardOverview();
    renderAdminUsersStats();
    renderAdminRecentPosts();
    renderAdminSounds();
  } catch (err) {
    if (hint) {
      hint.hidden = false;
      hint.textContent = `فشل تحميل لوحة الإدارة: ${err.data?.error || err.message}`;
    }
  }
}

function renderAdminSounds() {
  const list = el('adminSoundsList');
  if (!list) return;
  list.innerHTML = '';

  const sounds = Array.isArray(state.adminSounds) ? state.adminSounds : [];
  if (!sounds.length) {
    list.innerHTML = '<div class="muted">لا يوجد أصوات.</div>';
    return;
  }

  sounds.forEach((s) => {
    const div = document.createElement('div');
    div.className = 'item';
    div.innerHTML = `
      <div class="item__top">
        <div>
          <div class="item__title">${escapeHtml(String(s.title || ''))}</div>
          <div class="item__sub">${escapeHtml(String(s.url || ''))} • <span class="muted">${escapeHtml(
            String(s.id || '')
          )}</span></div>
        </div>
        <div class="row row--wrap">
          <button class="btn btn--ghost" data-del-sound="${escapeHtml(String(s.id || ''))}">حذف</button>
        </div>
      </div>
    `;

    div.querySelector('[data-del-sound]')?.addEventListener('click', async () => {
      const sid = String(s.id || '');
      if (!sid) return;
      if (!window.confirm('تأكيد حذف الصوت؟')) return;
      setAlert(el('adminSoundsAlert'), '', '');
      try {
        await apiFetch(`/admin/sounds/${encodeURIComponent(sid)}`, { method: 'DELETE' });
        const data = await apiFetch('/sounds');
        state.adminSounds = Array.isArray(data.sounds) ? data.sounds : [];
        renderAdminSounds();
        setAlert(el('adminSoundsAlert'), 'تم الحذف ✅', 'ok');
      } catch (err) {
        setAlert(el('adminSoundsAlert'), `فشل الحذف: ${err.data?.error || err.message}`, 'danger');
      }
    });

    list.appendChild(div);
  });
}

async function openUserProfile(userId) {
  if (!requireToken()) return;
  setAlert(el('otherAlert'), '', '');
  await loadOtherUser(userId);
  showView('user');
  const data = await apiFetch(`/posts?userId=${encodeURIComponent(userId)}`);
  renderPosts(data.posts || [], el('otherPostsList'));
  await refreshOtherHighlights();
}

async function loadMe() {
  const data = await apiFetch('/me');
  state.me = data.user;

  el('userChipName').textContent = state.me.username;
  el('userChipSub').textContent = state.me.email;
  el('userChipAvatar').src = state.me.avatarUrl || avatarFallback(state.me.username);

  el('profileName').innerHTML = `${escapeHtml(state.me.username)}${verifiedBadgeHtml(state.me.verified)}`;
  el('profileEmail').textContent = state.me.email;
  el('profileAvatar').src = state.me.avatarUrl || avatarFallback(state.me.username);
  el('followersCount').textContent = `المتابعين: ${(state.me.followers || []).length}`;
  el('followingCount').textContent = `المتابَعين: ${(state.me.following || []).length}`;

  el('editUsername').value = state.me.username || '';
  el('editBio').value = state.me.bio || '';
  el('editAvatarUrl').value = state.me.avatarUrl || '';
}

async function loadOtherUser(userId) {
  const data = await apiFetch(`/users/${encodeURIComponent(userId)}`);
  state.otherUser = data.user;

  el('otherName').innerHTML = `${escapeHtml(state.otherUser.username)}${verifiedBadgeHtml(state.otherUser.verified)}`;
  el('otherEmail').textContent = state.otherUser.email;
  el('otherAvatar').src = state.otherUser.avatarUrl || avatarFallback(state.otherUser.username);
  el('otherFollowers').textContent = `المتابعين: ${(state.otherUser.followers || []).length}`;
  el('otherFollowing').textContent = `المتابَعين: ${(state.otherUser.following || []).length}`;

  const isFollowing = (state.me?.following || []).includes(state.otherUser.id);
  el('otherFollowBtn').hidden = isFollowing;
  el('otherUnfollowBtn').hidden = !isFollowing;
}

function requireToken() {
  if (!state.token) {
    showView('auth');
    setLoggedInUI(false);
    return false;
  }
  return true;
}

function renderUsers(users) {
  const list = el('usersList');
  list.innerHTML = '';

  if (!users.length) {
    list.innerHTML = '<div class="muted">لا يوجد نتائج.</div>';
    return;
  }

  users.forEach((u) => {
    const div = document.createElement('div');
    div.className = 'item';
    div.innerHTML = `
      <div class="item__top">
        <div>
          <div class="item__title">${escapeHtml(u.username)}${verifiedBadgeHtml(u.verified)}</div>
          <div class="item__sub">${escapeHtml(u.email)} • <span class="muted">${escapeHtml(u.id)}</span></div>
        </div>
        <div class="row row--wrap">
          <button class="btn" data-follow="${u.id}">متابعة</button>
          <button class="btn btn--ghost" data-unfollow="${u.id}">إلغاء</button>
        </div>
      </div>
    `;

    div.querySelector('[data-follow]')?.addEventListener('click', async () => {
      await apiFetch(`/users/${u.id}/follow`, { method: 'POST' });
      await loadMe();
    });

    div.querySelector('[data-unfollow]')?.addEventListener('click', async () => {
      await apiFetch(`/users/${u.id}/unfollow`, { method: 'POST' });
      await loadMe();
    });

    list.appendChild(div);
  });
}

function renderPosts(posts, target) {
  target.innerHTML = '';

  if (!posts.length) {
    target.innerHTML = '<div class="muted">لا يوجد بوستات.</div>';
    return;
  }

  posts.forEach((p) => {
    const author = p.author || { username: 'Unknown', avatarUrl: '' };
    const wrap = document.createElement('div');
    wrap.className = 'post';

    const isMyProfilePosts = String(target?.id || '') === 'myPostsList' && String(state.activeView || '') === 'profile';
    const isMyOwnPostAnyView = String(p.userId || '') === String(state.me?.id || '');
    const isMyOwnPost = isMyProfilePosts && isMyOwnPostAnyView;
    const pinnedSlot = Number(p.pinnedSlot || 0);

    const comments = Array.isArray(p.comments) ? p.comments : [];
    const commentsHtml = comments.length
      ? `<div class="stack">${comments
          .slice(-8)
          .map((c) => {
            const u = c.user || { username: 'Unknown' };
            return `<div class="muted"><strong>${escapeHtml(u.username)}${verifiedBadgeHtml(u.verified)}:</strong> ${linkifyText(c.text || '')}</div>`;
          })
          .join('')}</div>`
      : '<div class="muted">لا يوجد تعليقات.</div>';

    const media = Array.isArray(p.media) ? p.media : [];
    const mediaHtml = media
      .slice(0, 6)
      .map((m) => {
        const type = String(m?.type || '');
        const url = String(m?.url || '');
        if (!url) return '';
        if (type.startsWith('video')) {
          return `<video class="postMedia" src="${url}" controls></video>`;
        }
        return `<img class="postMedia" alt="" src="${url}" />`;
      })
      .filter(Boolean)
      .join('');

    wrap.innerHTML = `
      <div class="post__head">
        <div class="post__author" data-user="${escapeHtml(author.id || '')}">
          <img class="post__avatar" alt="" data-user="${escapeHtml(author.id || '')}" src="${author.avatarUrl || avatarFallback(author.username)}" />
          <div class="post__meta">
            <div class="post__name" data-user="${escapeHtml(author.id || '')}">${escapeHtml(author.username)}${verifiedBadgeHtml(author.verified)}</div>
            <div class="post__time">${escapeHtml(formatTime(p.createdAt))}</div>
          </div>
        </div>
        <div class="pill">${pinnedSlot ? `📌 ${pinnedSlot} • ` : ''}❤️ ${p.likes?.length || 0} • 💾 ${p.saves?.length || 0} • 🔁 ${p.sharesCount || 0}</div>
      </div>
      <div class="post__text">${linkifyText(p.text || '')}</div>
      ${mediaHtml ? `<div class="postMediaGrid">${mediaHtml}</div>` : ''}
      ${p.sound?.url ? `<audio class="audioPlayer postAutoSound" data-autosound="1" preload="metadata" src="${escapeHtml(p.sound.url)}"></audio>` : ''}
      <div class="actions">
        <button class="btn" data-like="${p.id}">لايك</button>
        <button class="btn btn--ghost" data-unlike="${p.id}">إلغاء</button>
        <button class="btn" data-save="${p.id}">حفظ</button>
        <button class="btn btn--ghost" data-unsave="${p.id}">إلغاء حفظ</button>
        <button class="btn" data-share="${p.id}">شير</button>
        ${isMyOwnPostAnyView ? `<button class="btn btn--ghost" data-edit="${p.id}">تعديل</button>` : ''}
        <button class="btn" data-to-ai="${p.id}">إرسال للـ AI</button>
        ${isMyOwnPost ? (pinnedSlot ? `<button class="btn btn--ghost" data-unpin="${p.id}">إلغاء تثبيت</button>` : `<button class="btn" data-pin="${p.id}">تثبيت</button>`) : ''}
      </div>
      <div class="divider"></div>
      <div class="card__title">التعليقات</div>
      ${commentsHtml}
      <div class="divider"></div>
      <div class="row">
        <input class="input" placeholder="اكتب تعليق..." data-comment-input="${p.id}" />
        <button class="btn" data-comment="${p.id}">نشر</button>
      </div>
    `;

    wrap.addEventListener('click', async (e) => {
      const t = e.target;
      if (!(t instanceof Element)) return;
      const a = t.closest('a[data-mention], a[data-tag]');
      if (!a) return;
      e.preventDefault();
      e.stopPropagation();
      const mention = a.getAttribute('data-mention');
      const tag = a.getAttribute('data-tag');
      if (mention) {
        await openMentionProfile(mention);
        return;
      }
      if (tag) {
        await filterFeedByTag(tag);
      }
    });

    wrap.querySelector('[data-like]')?.addEventListener('click', async () => {
      await apiFetch(`/posts/${p.id}/like`, { method: 'POST' });
      await refreshFeed();
    });

    wrap.querySelector('[data-unlike]')?.addEventListener('click', async () => {
      await apiFetch(`/posts/${p.id}/unlike`, { method: 'POST' });
      await refreshFeed();
    });

    wrap.querySelector('[data-save]')?.addEventListener('click', async () => {
      await apiFetch(`/posts/${p.id}/save`, { method: 'POST' });
      await refreshFeed();
    });

    wrap.querySelector('[data-unsave]')?.addEventListener('click', async () => {
      await apiFetch(`/posts/${p.id}/unsave`, { method: 'POST' });
      await refreshFeed();
    });

    wrap.querySelector('[data-share]')?.addEventListener('click', async () => {
      await apiFetch(`/posts/${p.id}/share`, { method: 'POST' });
      await refreshFeed();
    });

    wrap.querySelector('[data-to-ai]')?.addEventListener('click', async () => {
      const msg = `حلّل البوست ده واقترح ردود أو عنوان:\n\n${p.text || ''}`;
      await openAiChat();
      await sendAiMessage(msg);
      showView('chats');
      await refreshChats();
    });

    wrap.querySelector('[data-edit]')?.addEventListener('click', async () => {
      const nextText = window.prompt('عدّل نص البوست:', String(p.text || ''));
      if (nextText === null) return;
      try {
        await apiFetch(`/posts/${encodeURIComponent(p.id)}`, {
          method: 'PATCH',
          body: JSON.stringify({ text: String(nextText) }),
        });
        await refreshActivePosts();
      } catch (err) {
        setAlert(el('feedAlert') || el('createAlert') || el('profileAlert'), `فشل التعديل: ${err.data?.error || err.message}`, 'danger');
      }
    });

    wrap.querySelector('[data-pin]')?.addEventListener('click', async () => {
      const pick = window.prompt('اختر ترتيب التثبيت (1-3) أو اتركه فارغ:', '1');
      const slot = pick === null ? null : pick.trim();
      const slotNum = slot ? Number(slot) : NaN;
      const payload = Number.isFinite(slotNum) && slotNum >= 1 && slotNum <= 3 ? { slot: slotNum } : {};
      await apiFetch(`/posts/${encodeURIComponent(p.id)}/pin`, { method: 'POST', body: JSON.stringify(payload) });
      await refreshMyPosts();
    });

    wrap.querySelector('[data-unpin]')?.addEventListener('click', async () => {
      await apiFetch(`/posts/${encodeURIComponent(p.id)}/unpin`, { method: 'POST' });
      await refreshMyPosts();
    });

    wrap.querySelector('[data-user]')?.addEventListener('click', async () => {
      if (!author.id) return;
      await openUserProfile(author.id);
    });

    wrap.querySelector('[data-comment]')?.addEventListener('click', async () => {
      const input = wrap.querySelector(`[data-comment-input="${p.id}"]`);
      const text = input?.value || '';
      if (!text.trim()) return;

      try {
        await apiFetch(`/posts/${p.id}/comments`, { method: 'POST', body: JSON.stringify({ text }) });
        input.value = '';
        await refreshActivePosts();
      } catch (err) {
        setAlert(el('feedAlert') || el('createAlert'), `فشل نشر التعليق: ${err.data?.error || err.message}`, 'danger');
      }
    });

    target.appendChild(wrap);
  });

  setupSoundsAutoplay(target);
}

function renderChats(chats) {
  const list = el('chatsList');
  list.innerHTML = '';

  if (!chats.length) {
    list.innerHTML = '<div class="muted">لا يوجد محادثات. افتح شات AI أو أنشئ Direct.</div>';
    return;
  }

  chats.forEach((c) => {
    const div = document.createElement('div');
    div.className = 'item';
    const title = c.displayTitle || (c.type === 'ai' ? '🤖 AI' : c.type === 'group' ? (c.title || 'Group') : 'Direct');
    div.innerHTML = `
      <div class="item__top">
        <div>
          <div class="item__title">${escapeHtml(title)}</div>
          <div class="item__sub">${escapeHtml(c.id)} • ${escapeHtml(formatTime(c.lastMessageAt))}</div>
        </div>
        <button class="btn" data-open="${c.id}">فتح</button>
      </div>
    `;

    div.querySelector('[data-open]')?.addEventListener('click', async () => {
      state.selectedChatId = c.id;
      await openChat(c.id);
    });

    list.appendChild(div);
  });
}

function renderMessages(messages) {
  const list = el('messagesList');
  list.innerHTML = '';

  if (!messages.length) {
    list.innerHTML = '<div class="muted">ابدأ المحادثة…</div>';
    return;
  }

  messages.forEach((m) => {
    const div = document.createElement('div');
    const isMe = m.senderId === state.me?.id;
    const isAi = m.senderId === 'ai';
    div.className = `msg ${isMe ? 'msg--me' : isAi ? 'msg--ai' : ''}`;
    div.innerHTML = `
      <div>${escapeHtml(m.text || '')}</div>
      <div class="msg__meta">${escapeHtml(formatTime(m.createdAt))}</div>
    `;
    list.appendChild(div);
  });

  list.scrollTop = list.scrollHeight;
}

async function refreshFeed() {
  if (!requireToken()) return;
  if (!state.me) await loadMe();
  await refreshStories();
  renderStoriesBar();
  const data = await apiFetch('/posts');
  renderPosts(data.posts || [], el('feedList'));
}

function renderReels(reels) {
  const list = el('reelsList');
  list.innerHTML = '';

  if (!Array.isArray(reels) || !reels.length) {
    list.innerHTML = '<div class="muted">لا يوجد ريلز.</div>';
    return;
  }

  reels.forEach((r) => {
    const author = r.author || { username: 'Unknown', avatarUrl: '' };
    const wrap = document.createElement('div');
    wrap.className = 'post';

    const m = Array.isArray(r.media) && r.media.length ? r.media[0] : null;
    const url = String(m?.url || '');

    wrap.innerHTML = `
      <div class="post__head">
        <div class="post__author" data-user="${escapeHtml(author.id || '')}">
          <img class="post__avatar" alt="" data-user="${escapeHtml(author.id || '')}" src="${author.avatarUrl || avatarFallback(author.username)}" />
          <div class="post__meta">
            <div class="post__name" data-user="${escapeHtml(author.id || '')}">${escapeHtml(author.username)}${verifiedBadgeHtml(author.verified)}</div>
            <div class="post__time">${escapeHtml(formatTime(r.createdAt))}</div>
          </div>
        </div>
        <div class="pill">❤️ ${r.likes?.length || 0} • 💾 ${r.saves?.length || 0}</div>
      </div>

      ${url ? `<video class="reelVideo" src="${url}" muted playsinline loop preload="metadata"></video>` : '<div class="muted">فيديو غير متاح.</div>'}
      ${r.text ? `<div class="post__text">${escapeHtml(r.text || '')}</div>` : ''}
      ${r.sound?.url ? `<audio class="audioPlayer postAutoSound" data-autosound="1" preload="metadata" src="${escapeHtml(r.sound.url)}"></audio>` : ''}

      <div class="actions">
        <button class="btn" data-like="${r.id}">لايك</button>
        <button class="btn btn--ghost" data-unlike="${r.id}">إلغاء</button>
        <button class="btn" data-save="${r.id}">حفظ</button>
        <button class="btn btn--ghost" data-unsave="${r.id}">إلغاء حفظ</button>
      </div>
    `;

    wrap.querySelector('[data-like]')?.addEventListener('click', async () => {
      await apiFetch(`/posts/${r.id}/like`, { method: 'POST' });
      await refreshReels();
    });
    wrap.querySelector('[data-unlike]')?.addEventListener('click', async () => {
      await apiFetch(`/posts/${r.id}/unlike`, { method: 'POST' });
      await refreshReels();
    });
    wrap.querySelector('[data-save]')?.addEventListener('click', async () => {
      await apiFetch(`/posts/${r.id}/save`, { method: 'POST' });
      await refreshReels();
    });
    wrap.querySelector('[data-unsave]')?.addEventListener('click', async () => {
      await apiFetch(`/posts/${r.id}/unsave`, { method: 'POST' });
      await refreshReels();
    });

    wrap.querySelector('[data-user]')?.addEventListener('click', async () => {
      if (!author.id) return;
      await openUserProfile(author.id);
    });

    list.appendChild(wrap);
  });

  setupReelsAutoplay();
  setupSoundsAutoplay(list);
}

async function refreshReels() {
  if (!requireToken()) return;
  const data = await apiFetch('/reels');
  renderReels(data.reels || []);
}

async function refreshOtherPosts() {
  if (!requireToken()) return;
  if (!state.otherUser?.id) return;
  const data = await apiFetch(`/posts?userId=${encodeURIComponent(state.otherUser.id)}`);
  renderPosts(data.posts || [], el('otherPostsList'));
}

async function refreshActivePosts() {
  if (state.activeView === 'feed') return refreshFeed();
  if (state.activeView === 'saved') return refreshSaved();
  if (state.activeView === 'profile') return refreshMyPosts();
  if (state.activeView === 'user') return refreshOtherPosts();
}

async function refreshMyPosts() {
  if (!requireToken()) return;
  const data = await apiFetch(`/posts?userId=${encodeURIComponent(state.me.id)}`);
  renderPosts(data.posts || [], el('myPostsList'));
}

async function searchUsers() {
  if (!requireToken()) return;
  const q = el('userSearchInput').value.trim();
  const data = await apiFetch(`/users?q=${encodeURIComponent(q)}`);
  renderUsers(data.users || []);
}

async function refreshSaved() {
  if (!requireToken()) return;
  try {
    const data = await apiFetch('/posts/saved');
    renderPosts(data.posts || [], el('savedPostsList'));
  } catch (err) {
    const target = el('savedPostsList');
    target.innerHTML = `<div class="muted">فشل تحميل المحفوظات: ${escapeHtml(err.data?.error || err.message)}</div>`;
  }
}

async function refreshNotifications() {
  if (!requireToken()) return;
  const data = await apiFetch('/notifications');
  const count = Number(data.unreadCount || 0);
  el('notifBadge').textContent = String(count);
  el('notifBadge').hidden = count <= 0;
  renderNotifications(data.notifications || []);
}

async function refreshChats() {
  if (!requireToken()) return;
  const data = await apiFetch('/chats');
  renderChats(data.chats || []);
}

async function openAiChat() {
  const data = await apiFetch('/chats/ai', { method: 'POST' });
  state.selectedChatId = data.chat.id;
  await openChat(data.chat.id);
}

async function openChat(chatId) {
  const data = await apiFetch(`/chats/${chatId}/messages`);
  const chat = data.chat;
  const title = chat.displayTitle || (chat.type === 'ai' ? '🤖 AI' : chat.type === 'group' ? (chat.title || 'Group') : 'Direct');
  el('chatTitle').textContent = title;
  renderMessages(data.messages || []);
}

async function sendMessageToChat(chatId, text) {
  const payload = { type: 'text', text };
  await apiFetch(`/chats/${chatId}/messages`, { method: 'POST', body: JSON.stringify(payload) });
}

async function sendAiMessage(text) {
  await apiFetch('/chats/ai/message', { method: 'POST', body: JSON.stringify({ text }) });
}

function escapeHtml(s) {
  return String(s)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function linkifyText(text) {
  const t = String(text || '');
  const escaped = escapeHtml(t);
  const withMentions = escaped.replace(/(^|\s)@([a-zA-Z0-9_\.]{2,30})/g, (m, pre, u) => {
    return `${pre}<a href="#" class="mentionLink" data-mention="${escapeHtml(u)}">@${escapeHtml(u)}</a>`;
  });
  const withTags = withMentions.replace(/(^|\s)#([\p{L}\p{N}_]{2,50})/gu, (m, pre, tag) => {
    return `${pre}<a href="#" class="hashtagLink" data-tag="${escapeHtml(tag)}">#${escapeHtml(tag)}</a>`;
  });
  return withTags;
}

async function openMentionProfile(username) {
  const q = String(username || '').trim();
  if (!q) return;
  try {
    const data = await apiFetch(`/users?q=${encodeURIComponent(q)}`);
    const users = Array.isArray(data.users) ? data.users : [];
    const u = users.find((x) => String(x.username || '').toLowerCase() === q.toLowerCase()) || users[0];
    if (u?.id) {
      await openUserProfile(u.id);
    }
  } catch {
    // ignore
  }
}

async function filterFeedByTag(tag) {
  const t = String(tag || '').trim().replace(/^#/, '');
  if (!t) return;
  showView('feed');
  if (!requireToken()) return;
  try {
    const data = await apiFetch(`/posts?tag=${encodeURIComponent(t)}`);
    renderPosts(data.posts || [], el('feedList'));
  } catch (err) {
    const target = el('feedList');
    target.innerHTML = `<div class="muted">فشل تحميل الهاشتاج: ${escapeHtml(err.data?.error || err.message)}</div>`;
  }
}

function getCurrentStory() {
  const groups = storyViewerState.groups || [];
  const g = groups[storyViewerState.groupIndex];
  return g?.stories?.[storyViewerState.storyIndex] || null;
}

function renderHighlights(list, target, onOpen) {
  if (!target) return;
  target.innerHTML = '';
  const items = Array.isArray(list) ? list : [];
  if (!items.length) {
    target.innerHTML = '<div class="muted">لا يوجد Highlights.</div>';
    return;
  }

  items.forEach((h) => {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'highlightBubble';
    const thumb = String(h?.coverUrl || h?.items?.[0]?.media?.url || '');
    btn.innerHTML = `
      <div class="highlightBubble__ring">
        <img class="highlightBubble__thumb" alt="" src="${thumb || avatarFallback(h?.title || 'H')}" />
      </div>
      <div class="highlightBubble__name">${escapeHtml(h?.title || '')}</div>
    `;
    btn.addEventListener('click', () => {
      try {
        onOpen && onOpen(h);
      } catch {
        // ignore
      }
    });
    target.appendChild(btn);
  });
}

async function refreshMyHighlights() {
  if (!requireToken()) return;
  if (!state.me?.id) return;
  try {
    const data = await apiFetch(`/users/${encodeURIComponent(state.me.id)}/highlights`);
    state.myHighlights = Array.isArray(data.highlights) ? data.highlights : [];
  } catch {
    state.myHighlights = [];
  }
  renderHighlights(state.myHighlights, el('myHighlightsList'), openHighlightViewer);
}

async function refreshOtherHighlights() {
  if (!requireToken()) return;
  if (!state.otherUser?.id) return;
  try {
    const data = await apiFetch(`/users/${encodeURIComponent(state.otherUser.id)}/highlights`);
    state.otherHighlights = Array.isArray(data.highlights) ? data.highlights : [];
  } catch {
    state.otherHighlights = [];
  }
  renderHighlights(state.otherHighlights, el('otherHighlightsList'), openHighlightViewer);
}

function closeHighlightViewer() {
  const modal = el('highlightModal');
  if (!modal) return;
  modal.hidden = true;
  stopStorySound();
  highlightViewerState.open = false;
  highlightViewerState.highlight = null;
  highlightViewerState.index = 0;
}

function renderHighlightItem() {
  const body = el('highlightViewerBody');
  const title = el('highlightViewerTitle');
  if (!body) return;
  const h = highlightViewerState.highlight;
  const items = Array.isArray(h?.items) ? h.items : [];
  const it = items[highlightViewerState.index];
  if (!it) {
    closeHighlightViewer();
    return;
  }

  stopStorySound();
  if (it?.sound?.url) {
    void playStorySound(it.sound.url);
  }

  if (title) {
    title.textContent = h?.title ? String(h.title) : 'Highlight';
  }

  const rendered = renderStoryCanvas(body, it);
  if (rendered?.kind === 'video' && rendered.el) {
    const v = rendered.el;
    v.controls = true;
    v.muted = false;
    try {
      void v.play();
    } catch {
      // ignore
    }
  }
}

function openHighlightViewer(highlight) {
  const modal = el('highlightModal');
  if (!modal) return;
  highlightViewerState.open = true;
  highlightViewerState.highlight = highlight || null;
  highlightViewerState.index = 0;
  modal.hidden = false;
  renderHighlightItem();
}

function goToNextHighlightItem() {
  const h = highlightViewerState.highlight;
  const items = Array.isArray(h?.items) ? h.items : [];
  if (!items.length) {
    closeHighlightViewer();
    return;
  }
  const next = highlightViewerState.index + 1;
  if (next >= items.length) {
    closeHighlightViewer();
    return;
  }
  highlightViewerState.index = next;
  renderHighlightItem();
}

function goToPrevHighlightItem() {
  const h = highlightViewerState.highlight;
  const items = Array.isArray(h?.items) ? h.items : [];
  if (!items.length) {
    closeHighlightViewer();
    return;
  }
  const prev = highlightViewerState.index - 1;
  if (prev < 0) {
    closeHighlightViewer();
    return;
  }
  highlightViewerState.index = prev;
  renderHighlightItem();
}

function renderStoryComments(comments) {
  const list = el('storyCommentsList');
  if (!list) return;
  list.innerHTML = '';
  const items = Array.isArray(comments) ? comments : [];
  if (!items.length) {
    list.innerHTML = '<div class="muted" style="color:rgba(255,255,255,.75)">لا يوجد تعليقات.</div>';
    return;
  }
  items.slice(0, 40).forEach((c) => {
    const u = c.user || { username: 'Unknown', avatarUrl: '' };
    const div = document.createElement('div');
    div.className = 'storyComment';
    div.innerHTML = `
      <img class="storyComment__avatar" alt="" src="${u.avatarUrl || avatarFallback(u.username)}" />
      <div class="storyComment__bubble"><span class="storyComment__name">${escapeHtml(u.username || '')}${verifiedBadgeHtml(u.verified)}</span>${linkifyText(c.text || '')}</div>
    `;
    div.querySelectorAll('[data-mention]')?.forEach((a) => {
      a.addEventListener('click', async (e) => {
        e.preventDefault();
        await openMentionProfile(a.dataset.mention);
      });
    });
    div.querySelectorAll('[data-tag]')?.forEach((a) => {
      a.addEventListener('click', async (e) => {
        e.preventDefault();
        await filterFeedByTag(a.dataset.tag);
      });
    });
    list.appendChild(div);
  });
}

async function refreshStoryComments() {
  const s = getCurrentStory();
  if (!s?.id) return;
  try {
    const data = await apiFetch(`/stories/${encodeURIComponent(String(s.id))}/comments`);
    renderStoryComments(data.comments || []);
  } catch {
    renderStoryComments([]);
  }
}

function verifiedBadgeHtml(verified) {
  if (!verified) return '';
  return `<span class="verifiedBadge" aria-label="موثق" title="موثق"><svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M20 6L9 17l-5-5" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/></svg></span>`;
}

function setAuthTab(mode) {
  const login = mode === 'login';
  el('tabLogin').classList.toggle('tab--active', login);
  el('tabRegister').classList.toggle('tab--active', !login);
  el('loginForm').hidden = !login;
  el('registerForm').hidden = login;
  setAlert(el('authAlert'), '', '');
}
function wireEvents() {
  document.querySelectorAll('.nav__item').forEach((btn) => {
    btn.addEventListener('click', async () => {
      const v = btn.dataset.view;
      if (!v) return;
      showView(v);
      if (v === 'reels') await refreshReels();
      if (v === 'meetings') {
        await refreshMeetingDevicesUI();
        const mid = getMeetingIdFromUrl();
        if (mid) await openMeeting(mid);
      }
      if (v === 'feed') await refreshFeed();
      if (v === 'create') await refreshCreateSoundsUI();
      if (v === 'saved') await refreshSaved();
      if (v === 'profile') await refreshMyHighlights();
      if (v === 'chats') await refreshChats();
      if (v === 'notifications') await refreshNotifications();
      if (v === 'dashboard') {
        window.history.pushState({}, '', '/dashbord');
        await refreshDashboard();
      } else if (window.location.pathname === '/dashbord') {
        window.history.replaceState({}, '', '/');
      }
      if (v === 'profile') {
        await loadMe();
        await refreshMyPosts();
        await refreshMyHighlights();
      }
    });
  });

  el('refreshReelsBtn')?.addEventListener('click', refreshReels);
  el('refreshDashboardBtn')?.addEventListener('click', refreshDashboard);

  el('adminUserSearchBtn')?.addEventListener('click', () => {
    renderAdminUsersStats();
  });

  el('adminFilterAllBtn')?.addEventListener('click', () => {
    state.adminPostsFilter = 'all';
    renderAdminRecentPosts();
  });
  el('adminFilterPostsBtn')?.addEventListener('click', () => {
    state.adminPostsFilter = 'post';
    renderAdminRecentPosts();
  });
  el('adminFilterReelsBtn')?.addEventListener('click', () => {
    state.adminPostsFilter = 'reel';
    renderAdminRecentPosts();
  });

  el('adminSendNotifyBtn')?.addEventListener('click', async () => {
    setAlert(el('adminNotifyAlert'), '', '');
    try {
      const userId = el('adminNotifyUserId').value.trim();
      const title = el('adminNotifyTitle').value.trim();
      const message = el('adminNotifyMessage').value.trim();
      if (!userId || !message) {
        setAlert(el('adminNotifyAlert'), 'اكتب User ID والرسالة.', 'danger');
        return;
      }
      await apiFetch('/admin/notify', {
        method: 'POST',
        body: JSON.stringify({ userId, title, message, level: 'warn' }),
      });
      setAlert(el('adminNotifyAlert'), 'تم الإرسال ✅', 'ok');
      await refreshNotifications();
    } catch (err) {
      setAlert(el('adminNotifyAlert'), `فشل الإرسال: ${err.data?.error || err.message}`, 'danger');
    }
  });

  function setCreateMode(mode) {
    state.createMode = mode;
    const isPost = mode === 'post';
    const isReel = mode === 'reel';
    const isStory = mode === 'story';
    el('createModePostBtn')?.classList.toggle('tab--active', isPost);
    el('createModeReelBtn')?.classList.toggle('tab--active', isReel);
    el('createModeStoryBtn')?.classList.toggle('tab--active', isStory);

    el('createPostWrap') && (el('createPostWrap').hidden = isStory);
    el('createStoryWrap') && (el('createStoryWrap').hidden = !isStory);
    el('soundPickerWrap') && (el('soundPickerWrap').hidden = false);

    const media = el('postMedia');
    if (media) {
      media.multiple = !isReel;
      media.accept = isReel ? 'video/*' : 'image/*,video/*';
    }
    state.postMediaDraft = [];
    state.storyMediaDraft = null;
    state.storyStyleDraft = { bg: '#0b0d10', color: '#ffffff', fontSize: 32, x: 0.5, y: 0.5, mediaScale: 1 };
    state.storyInteractiveDraft = null;
    if (media) media.value = '';
    el('postMediaPreview').hidden = true;
    el('postMediaPreview').innerHTML = '';

    if (el('storyMedia')) el('storyMedia').value = '';
    if (el('storyText')) el('storyText').value = '';
    if (el('storyBg')) el('storyBg').value = '#0b0d10';
    if (el('storyColor')) el('storyColor').value = '#ffffff';
    if (el('storyFontSize')) el('storyFontSize').value = '32';
    if (el('storyMediaScale')) el('storyMediaScale').value = '1';
    if (el('storyInteractiveKind')) el('storyInteractiveKind').value = '';
    if (el('storyInteractiveQuestion')) el('storyInteractiveQuestion').value = '';
    if (el('storyPollOpt1')) el('storyPollOpt1').value = '';
    if (el('storyPollOpt2')) el('storyPollOpt2').value = '';
    if (el('storyPollOpt3')) el('storyPollOpt3').value = '';
    if (el('storyPollOpt4')) el('storyPollOpt4').value = '';
    if (el('storyInteractiveFields')) el('storyInteractiveFields').hidden = true;
    if (el('storyPollFields')) el('storyPollFields').hidden = true;
    try {
      renderStoryStage();
    } catch {
      // ignore
    }
  }

  el('createModePostBtn')?.addEventListener('click', () => setCreateMode('post'));
  el('createModeReelBtn')?.addEventListener('click', () => setCreateMode('reel'));
  el('createModeStoryBtn')?.addEventListener('click', () => setCreateMode('story'));

  el('soundSelect')?.addEventListener('change', () => {
    state.selectedSoundId = String(el('soundSelect')?.value || '');
    renderSoundPicker();
  });

  try {
    const storyControls = ['storyText', 'storyBg', 'storyColor', 'storyFontSize', 'storyMediaScale'];
    storyControls.forEach((id) => {
      el(id)?.addEventListener(id === 'storyText' ? 'input' : 'change', () => {
        const st = getStoryStyleFromUI();
        state.storyStyleDraft = st;
        renderStoryStage();
      });
    });

    const updateInteractiveUI = () => {
      const kind = String(el('storyInteractiveKind')?.value || '').trim().toLowerCase();
      if (el('storyInteractiveFields')) el('storyInteractiveFields').hidden = !kind;
      if (el('storyInteractiveQuestionWrap')) {
        el('storyInteractiveQuestionWrap').hidden = !['poll', 'question', 'quiz', 'slider'].includes(kind);
      }
      if (el('storyPollFields')) el('storyPollFields').hidden = !['poll', 'quiz'].includes(kind);
      if (el('storyQuizFields')) el('storyQuizFields').hidden = kind !== 'quiz';
      if (el('storySliderFields')) el('storySliderFields').hidden = kind !== 'slider';
      if (el('storyLinkFields')) el('storyLinkFields').hidden = kind !== 'link';
      if (el('storyMentionFields')) el('storyMentionFields').hidden = kind !== 'mention';
      if (el('storyLocationFields')) el('storyLocationFields').hidden = kind !== 'location';
      if (el('storyCountdownFields')) el('storyCountdownFields').hidden = kind !== 'countdown';
      renderStoryStage();
    };

    el('storyInteractiveKind')?.addEventListener('change', updateInteractiveUI);
    el('storyInteractiveQuestion')?.addEventListener('input', updateInteractiveUI);
    ['storyPollOpt1', 'storyPollOpt2', 'storyPollOpt3', 'storyPollOpt4', 'storyQuizCorrectIndex', 'storySliderEmoji', 'storyLinkTitle', 'storyLinkUrl', 'storyMentionUsername', 'storyLocationName', 'storyCountdownTitle', 'storyCountdownEndAt'].forEach((id) => {
      el(id)?.addEventListener('input', updateInteractiveUI);
    });

    let storyDragActive = false;
    let storyDragPointerId = null;
    el('storyStageText')?.addEventListener('pointerdown', (e) => {
      const stage = el('storyStage');
      const textBox = el('storyStageText');
      if (!stage || !textBox || textBox.hidden) return;
      storyDragActive = true;
      storyDragPointerId = e.pointerId;
      try {
        textBox.setPointerCapture(e.pointerId);
      } catch {
        // ignore
      }
      e.preventDefault();
    });
    el('storyStageText')?.addEventListener('pointermove', (e) => {
      if (!storyDragActive) return;
      if (storyDragPointerId != null && e.pointerId !== storyDragPointerId) return;
      const stage = el('storyStage');
      const textBox = el('storyStageText');
      if (!stage || !textBox) return;
      const r = stage.getBoundingClientRect();
      const x = (e.clientX - r.left) / Math.max(1, r.width);
      const y = (e.clientY - r.top) / Math.max(1, r.height);
      state.storyStyleDraft.x = clamp01(x);
      state.storyStyleDraft.y = clamp01(y);
      textBox.style.left = `${Math.round(state.storyStyleDraft.x * 100)}%`;
      textBox.style.top = `${Math.round(state.storyStyleDraft.y * 100)}%`;
    });
    el('storyStageText')?.addEventListener('pointerup', (e) => {
      if (storyDragPointerId != null && e.pointerId !== storyDragPointerId) return;
      storyDragActive = false;
      storyDragPointerId = null;
    });
    el('storyStageText')?.addEventListener('pointercancel', () => {
      storyDragActive = false;
      storyDragPointerId = null;
    });
  } catch {
    // ignore
  }

  el('refreshSavedBtn').addEventListener('click', refreshSaved);
  el('refreshNotifBtn').addEventListener('click', refreshNotifications);

  el('notifBtn').addEventListener('click', async () => {
    showView('notifications');
    await refreshNotifications();
  });

  el('openAiChatBtn').addEventListener('click', async () => {
    setAlert(el('chatAlert'), '', '');
    try {
      await openAiChat();
      showView('chats');
      await refreshChats();
    } catch (err) {
      setAlert(el('chatAlert'), `فشل: ${err.data?.error || err.message}`, 'danger');
    }
  });

  el('backToFeedBtn').addEventListener('click', async () => {
    showView('feed');
    await refreshFeed();
  });

  el('tabLogin').addEventListener('click', () => setAuthTab('login'));
  el('tabRegister').addEventListener('click', () => setAuthTab('register'));

  el('loginForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    const fd = new FormData(e.target);
    try {
      const data = await apiFetch('/auth/login', {
        method: 'POST',
        body: JSON.stringify({ email: fd.get('email'), password: fd.get('password') }),
      });
      state.token = data.token;
      localStorage.setItem('nc_token', state.token);
      setLoggedInUI(true);
      await loadMe();
      const mid = getMeetingIdFromUrl();
      if (mid) {
        showView('meetings');
        await openMeeting(mid);
      } else {
        showView('feed');
        await refreshFeed();
      }
      await refreshNotifications();
      setChatPolling(true);
      startAppPolling();
    } catch (err) {
      setAlert(el('authAlert'), `خطأ: ${err.data?.error || err.message}`, 'danger');
    }
  });

  el('registerForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    const fd = new FormData(e.target);
    try {
      const data = await apiFetch('/auth/register', {
        method: 'POST',
        body: JSON.stringify({
          username: fd.get('username'),
          email: fd.get('email'),
          password: fd.get('password'),
        }),
      });
      state.token = data.token;
      localStorage.setItem('nc_token', state.token);
      setLoggedInUI(true);
      await loadMe();
      const mid = getMeetingIdFromUrl();
      if (mid) {
        showView('meetings');
        await openMeeting(mid);
      } else {
        showView('feed');
        await refreshFeed();
      }
      await refreshNotifications();
      setChatPolling(true);
      startAppPolling();
    } catch (err) {
      setAlert(el('authAlert'), `خطأ: ${err.data?.error || err.message}`, 'danger');
    }
  });

  el('logoutBtn').addEventListener('click', async () => {
    try {
      if (state.token) await apiFetch('/auth/logout', { method: 'POST' });
    } catch {
      // ignore
    }
    try {
      stopMeeting();
    } catch {
      // ignore
    }
    state.token = '';
    state.me = null;
    localStorage.removeItem('nc_token');
    setLoggedInUI(false);
    showView('auth');
    setChatPolling(false);
    stopAppPolling();
  });

  el('refreshFeedBtn').addEventListener('click', refreshFeed);
  el('userSearchBtn').addEventListener('click', searchUsers);

  el('meetingCreateBtn')?.addEventListener('click', async () => {
    if (!requireToken()) return;
    setAlert(el('meetingAlert'), '', '');
    try {
      const title = String(el('meetingTitleInput')?.value || '').trim();
      const data = await apiFetch('/meetings', { method: 'POST', body: JSON.stringify({ title }) });
      const m = data.meeting;
      if (!m?.id) throw new Error('missing_meeting_id');
      showView('meetings');
      await openMeeting(String(m.id));
      setAlert(el('meetingAlert'), 'تم إنشاء الميتنج ✅', 'ok');
    } catch (err) {
      setAlert(el('meetingAlert'), `فشل: ${err.data?.error || err.message}`, 'danger');
    }
  });

  el('meetingCopyLinkBtn')?.addEventListener('click', async () => {
    if (!meetingState.id) return;
    const link = `${window.location.origin}/?meeting=${encodeURIComponent(meetingState.id)}`;
    try {
      await navigator.clipboard.writeText(link);
      setAlert(el('meetingRoomAlert'), 'تم نسخ الرابط ✅', 'ok');
    } catch {
      try {
        window.prompt('Copy link:', link);
      } catch {
        // ignore
      }
    }
  });

  el('meetingHangupBtn')?.addEventListener('click', () => {
    stopMeeting();
    setAlert(el('meetingRoomAlert'), 'تم إنهاء المكالمة.', '');
  });

  el('meetingCamSelect')?.addEventListener('change', async () => {
    await ensureMeetingLocalMedia();
    updateMeetingControlLabels();
  });
  el('meetingMicSelect')?.addEventListener('change', async () => {
    await ensureMeetingLocalMedia();
    updateMeetingControlLabels();
  });

  el('meetingToggleCamBtn')?.addEventListener('click', () => {
    const t = meetingState.localStream?.getVideoTracks?.()[0];
    if (!t) return;
    t.enabled = !t.enabled;
    updateMeetingControlLabels();
  });

  el('meetingToggleMicBtn')?.addEventListener('click', () => {
    const t = meetingState.localStream?.getAudioTracks?.()[0];
    if (!t) return;
    t.enabled = !t.enabled;
    updateMeetingControlLabels();
  });

  el('meetingMuteRemoteBtn')?.addEventListener('click', () => {
    meetingState.remoteMuted = !meetingState.remoteMuted;
    meetingState.remoteEls.forEach((wrap) => {
      try {
        const v = wrap.querySelector('[data-peer-video]');
        if (v instanceof HTMLVideoElement) v.muted = Boolean(meetingState.remoteMuted);
      } catch {
        // ignore
      }
    });
    try {
      const mainV = el('meetingRemoteMainVideo');
      if (mainV instanceof HTMLVideoElement) mainV.muted = Boolean(meetingState.remoteMuted);
    } catch {
      // ignore
    }
    updateMeetingControlLabels();
  });

  el('meetingShareScreenBtn')?.addEventListener('click', async () => {
    if (!meetingState.localStream) return;
    if (meetingState.sharingScreen) {
      stopMeetingScreenShare();
      return;
    }
    await startMeetingScreenShare();
  });

  const sendMeetingChat = () => {
    if (!meetingState.ws || meetingState.ws.readyState !== 1) return;
    if (!meetingState.approved) {
      setAlert(el('meetingRoomAlert'), 'لازم موافقة صاحب الميتنج الأول قبل الشات.', 'danger');
      return;
    }
    const input = el('meetingChatInput');
    const text = String(input?.value || '').trim();
    if (!text) return;

    const clientId =
      typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function'
        ? crypto.randomUUID()
        : `c_${Date.now()}_${Math.random().toString(16).slice(2)}`;

    pushMeetingChatMessage({
      id: `local_${clientId}`,
      clientId,
      user: state.me || { username: 'Me' },
      userId: meetingState.meId || undefined,
      text,
      createdAt: new Date().toISOString(),
    });

    meetingSend({ type: 'chat_message', text, clientId });
    try {
      input.value = '';
    } catch {
      // ignore
    }
  };

  el('meetingChatSendBtn')?.addEventListener('click', sendMeetingChat);
  el('meetingChatInput')?.addEventListener('keydown', (e) => {
    if (e.key !== 'Enter') return;
    try {
      e.preventDefault();
    } catch {
      // ignore
    }
    sendMeetingChat();
  });

  el('meetingReactionsRow')?.querySelectorAll('[data-meeting-reaction]')?.forEach((b) => {
    b.addEventListener('click', () => {
      if (!meetingState.ws || meetingState.ws.readyState !== 1) return;
      if (!meetingState.approved) return;
      const reaction = String(b.getAttribute('data-meeting-reaction') || '').trim();
      if (!reaction) return;
      meetingSend({ type: 'reaction', reaction });
    });
  });

  el('meetingRecordBtn')?.addEventListener('click', () => {
    if (!meetingState.localStream) return;
    if (meetingState.recordingActive) {
      stopMeetingRecording();
      return;
    }
    startMeetingRecording();
  });

  const requestMeetingFullscreen = async () => {
    const remote = el('meetingRemoteMainVideo');
    const local = el('meetingLocalVideo');
    const target = remote instanceof HTMLVideoElement && remote.srcObject ? remote : local;
    if (!(target instanceof HTMLVideoElement)) return;
    try {
      if (typeof target.requestFullscreen === 'function') {
        await target.requestFullscreen();
      }
    } catch {
      // ignore
    }
  };

  el('meetingFullscreenBtn')?.addEventListener('click', requestMeetingFullscreen);
  el('meetingRemoteMainVideo')?.addEventListener('dblclick', requestMeetingFullscreen);

  el('postMedia').addEventListener('change', async (e) => {
    if (state.createMode === 'story') {
      e.target.value = '';
      return;
    }
    const files = Array.from(e.target.files || []);
    state.postMediaDraft = [];
    if (!files.length) {
      el('postMediaPreview').hidden = true;
      el('postMediaPreview').innerHTML = '';
      return;
    }

    const isReel = state.createMode === 'reel';
    const maxTotalBytes = isReel ? 25 * 1024 * 1024 : 45 * 1024 * 1024;
    const maxSingleBytes = isReel ? 25 * 1024 * 1024 : 20 * 1024 * 1024;
    const totalBytes = files.reduce((sum, f) => sum + Number(f.size || 0), 0);
    const tooBig = files.find((f) => Number(f.size || 0) > maxSingleBytes);
    if (tooBig || totalBytes > maxTotalBytes) {
      setAlert(el('createAlert'), 'الملفات كبيرة جدًا. قلّل حجم الفيديو/الصور ثم جرّب مرة أخرى.', 'danger');
      e.target.value = '';
      el('postMediaPreview').hidden = true;
      el('postMediaPreview').innerHTML = '';
      return;
    }

    if (isReel) {
      if (files.length !== 1) {
        setAlert(el('createAlert'), 'الريل لازم يكون فيديو واحد فقط.', 'danger');
        e.target.value = '';
        return;
      }
      const t = String(files[0]?.type || '');
      if (!t.startsWith('video')) {
        setAlert(el('createAlert'), 'اختار فيديو للريل.', 'danger');
        e.target.value = '';
        return;
      }
    }

    const items = await Promise.all(files.map(readFileAsDataUrl));
    state.postMediaDraft = items
      .filter((x) => x && x.url)
      .slice(0, 6);

    renderMediaPreview();
  });

  el('storyMedia')?.addEventListener('change', async (e) => {
    const file = Array.from(e.target.files || [])[0];
    state.storyMediaDraft = null;
    if (!file) {
      renderStoryStage();
      return;
    }

    const maxBytes = 25 * 1024 * 1024;
    if (Number(file.size || 0) > maxBytes) {
      setAlert(el('createAlert'), 'الملف كبير جدًا للستوري. جرّب ملف أصغر.', 'danger');
      e.target.value = '';
      renderStoryStage();
      return;
    }

    const item = await readFileAsDataUrl(file);
    if (!item || !item.url) {
      setAlert(el('createAlert'), 'فشل قراءة ملف الستوري.', 'danger');
      e.target.value = '';
      return;
    }

    const t = String(item.type || '');
    if (!t.startsWith('image') && !t.startsWith('video')) {
      setAlert(el('createAlert'), 'اختار صورة أو فيديو فقط.', 'danger');
      e.target.value = '';
      return;
    }

    state.storyMediaDraft = item;
    renderStoryStage();
  });

  el('publishPostBtn').addEventListener('click', async () => {
    if (state.isPublishing) return;
    state.isPublishing = true;
    el('publishPostBtn').disabled = true;
    setAlert(el('createAlert'), '', '');
    const text = el('postText').value;
    try {
      if (state.createMode === 'story') {
        const storyText = String(el('storyText')?.value || '');
        const style = getStoryStyleFromUI();
        state.storyStyleDraft = style;

        if (!String(storyText || '').trim() && !state.storyMediaDraft?.url) {
          setAlert(el('createAlert'), 'اكتب نص للستوري أو اختر صورة/فيديو.', 'danger');
          return;
        }

        const payload = { text: storyText, style };
        if (state.storyMediaDraft?.url) {
          payload.media = {
            type: String(state.storyMediaDraft.type || ''),
            url: String(state.storyMediaDraft.url || ''),
          };
        }
        if (state.selectedSoundId) {
          payload.soundId = String(state.selectedSoundId);
        }

        const interactiveDraft = getStoryInteractiveDraftFromUI();
        if (interactiveDraft && interactiveDraft.kind) {
          const k = String(interactiveDraft.kind || '').trim().toLowerCase();
          if (['poll', 'question', 'quiz', 'slider'].includes(k) && !String(interactiveDraft.question || '').trim()) {
            setAlert(el('createAlert'), 'اكتب السؤال للـ Sticker.', 'danger');
            state.isPublishing = false;
            el('publishPostBtn').disabled = false;
            return;
          }
          if (k === 'poll' && (!Array.isArray(interactiveDraft.options) || interactiveDraft.options.length < 2)) {
            setAlert(el('createAlert'), 'اكتب على الأقل اختيارين للـ Poll.', 'danger');
            state.isPublishing = false;
            el('publishPostBtn').disabled = false;
            return;
          }
          if (k === 'quiz' && (!Array.isArray(interactiveDraft.options) || interactiveDraft.options.length < 2)) {
            setAlert(el('createAlert'), 'اكتب على الأقل اختيارين للـ Quiz.', 'danger');
            state.isPublishing = false;
            el('publishPostBtn').disabled = false;
            return;
          }
          if (k === 'link') {
            const url = String(interactiveDraft.url || '').trim();
            if (!/^https?:\/\//i.test(url)) {
              setAlert(el('createAlert'), 'اكتب رابط صحيح يبدأ بـ https://', 'danger');
              state.isPublishing = false;
              el('publishPostBtn').disabled = false;
              return;
            }
          }
          if (k === 'mention' && !String(interactiveDraft.username || '').trim()) {
            setAlert(el('createAlert'), 'اكتب Username للـ Mention.', 'danger');
            state.isPublishing = false;
            el('publishPostBtn').disabled = false;
            return;
          }
          if (k === 'location' && !String(interactiveDraft.name || '').trim()) {
            setAlert(el('createAlert'), 'اكتب اسم المكان.', 'danger');
            state.isPublishing = false;
            el('publishPostBtn').disabled = false;
            return;
          }
          if (k === 'countdown') {
            if (!String(interactiveDraft.title || '').trim() || !String(interactiveDraft.endAt || '').trim()) {
              setAlert(el('createAlert'), 'اكتب عنوان ووقت انتهاء للـ Countdown.', 'danger');
              state.isPublishing = false;
              el('publishPostBtn').disabled = false;
              return;
            }
          }
          payload.interactive = interactiveDraft;
        }
        await apiFetch('/stories', {
          method: 'POST',
          body: JSON.stringify(payload),
        });

        if (el('storyMedia')) el('storyMedia').value = '';
        state.storyMediaDraft = null;
        if (el('storyText')) el('storyText').value = '';
        if (el('storyBg')) el('storyBg').value = '#0b0d10';
        if (el('storyColor')) el('storyColor').value = '#ffffff';
        if (el('storyFontSize')) el('storyFontSize').value = '32';
        if (el('storyMediaScale')) el('storyMediaScale').value = '1';
        state.storyStyleDraft = { bg: '#0b0d10', color: '#ffffff', fontSize: 32, x: 0.5, y: 0.5, mediaScale: 1 };
        state.selectedSoundId = '';
        state.storyInteractiveDraft = null;
        if (el('soundSelect')) el('soundSelect').value = '';
        if (el('soundPreview')) {
          el('soundPreview').hidden = true;
          el('soundPreview').src = '';
        }

        if (el('storyInteractiveKind')) el('storyInteractiveKind').value = '';
        if (el('storyInteractiveQuestion')) el('storyInteractiveQuestion').value = '';
        if (el('storyPollOpt1')) el('storyPollOpt1').value = '';
        if (el('storyPollOpt2')) el('storyPollOpt2').value = '';
        if (el('storyPollOpt3')) el('storyPollOpt3').value = '';
        if (el('storyPollOpt4')) el('storyPollOpt4').value = '';
        if (el('storyQuizCorrectIndex')) el('storyQuizCorrectIndex').value = '';
        if (el('storySliderEmoji')) el('storySliderEmoji').value = '';
        if (el('storyLinkTitle')) el('storyLinkTitle').value = '';
        if (el('storyLinkUrl')) el('storyLinkUrl').value = '';
        if (el('storyMentionUsername')) el('storyMentionUsername').value = '';
        if (el('storyLocationName')) el('storyLocationName').value = '';
        if (el('storyCountdownTitle')) el('storyCountdownTitle').value = '';
        if (el('storyCountdownEndAt')) el('storyCountdownEndAt').value = '';
        if (el('storyInteractiveFields')) el('storyInteractiveFields').hidden = true;
        if (el('storyPollFields')) el('storyPollFields').hidden = true;
        if (el('storyQuizFields')) el('storyQuizFields').hidden = true;
        if (el('storySliderFields')) el('storySliderFields').hidden = true;
        if (el('storyLinkFields')) el('storyLinkFields').hidden = true;
        if (el('storyMentionFields')) el('storyMentionFields').hidden = true;
        if (el('storyLocationFields')) el('storyLocationFields').hidden = true;
        if (el('storyCountdownFields')) el('storyCountdownFields').hidden = true;
        renderStoryStage();
        setAlert(el('createAlert'), 'تم نشر الستوري ✅', 'ok');
        showView('feed');
        await refreshFeed();
        return;
      }

      const payload = { text, media: state.postMediaDraft, kind: state.createMode };
      if (state.selectedSoundId) {
        payload.soundId = String(state.selectedSoundId);
      }
      await apiFetch('/posts', {
        method: 'POST',
        body: JSON.stringify(payload),
      });
      el('postText').value = '';
      el('postMedia').value = '';
      state.postMediaDraft = [];
      state.selectedSoundId = '';
      el('soundSelect') && (el('soundSelect').value = '');
      el('soundPreview') && (el('soundPreview').hidden = true);
      el('soundPreview') && (el('soundPreview').src = '');
      el('postMediaPreview').hidden = true;
      el('postMediaPreview').innerHTML = '';
      setAlert(el('createAlert'), state.createMode === 'reel' ? 'تم نشر الريل ✅' : 'تم نشر البوست ✅', 'ok');
    } catch (err) {
      const reason = err.data?.error || err.message;
      const msg =
        err.status === 413 || reason === 'payload_too_large'
          ? 'فشل النشر: حجم الفيديو/الصور كبير جدًا. جرّب فيديو أصغر.'
          : reason === 'invalid_sound'
            ? 'فشل النشر: الصوت غير صالح.'
          : `فشل النشر: ${reason}`;
      setAlert(el('createAlert'), msg, 'danger');
    } finally {
      state.isPublishing = false;
      el('publishPostBtn').disabled = false;
    }
  });

  el('createAddSoundBtn')?.addEventListener('click', async () => {
    setAlert(el('createSoundAlert'), '', '');
    try {
      if (!state.me) await loadMe();
      if (!state.me?.verified) {
        setAlert(el('createSoundAlert'), 'هذه الميزة متاحة للحسابات الموثّقة فقط.', 'danger');
        return;
      }

      const title = String(el('createSoundTitle')?.value || '').trim();
      const url = String(el('createSoundUrl')?.value || '').trim();
      if (!title || !url) {
        setAlert(el('createSoundAlert'), 'اكتب عنوان ورابط الصوت.', 'danger');
        return;
      }

      await apiFetch('/sounds', { method: 'POST', body: JSON.stringify({ title, url }) });
      el('createSoundTitle').value = '';
      el('createSoundUrl').value = '';

      await refreshSounds();
      renderSoundPicker();
      setAlert(el('createSoundAlert'), 'تمت إضافة الصوت ✅', 'ok');
    } catch (err) {
      const reason = err.data?.error || err.message;
      const msg =
        reason === 'sound_add_requires_verified'
          ? 'هذه الميزة متاحة للحسابات الموثّقة فقط.'
          : `فشل إضافة الصوت: ${reason}`;
      setAlert(el('createSoundAlert'), msg, 'danger');
    }
  });

  el('createStoryFromFeedBtn')?.addEventListener('click', async () => {
    showView('create');
    await refreshCreateSoundsUI();
    try {
      el('createModeStoryBtn')?.click();
    } catch {
      // ignore
    }
  });

  el('closeStoryBtn')?.addEventListener('click', closeStoryViewer);
  el('storyNextBtn')?.addEventListener('click', goToNextStory);
  el('storyPrevBtn')?.addEventListener('click', goToPrevStory);
  el('storyModal')?.addEventListener('click', (e) => {
    if (e.target === el('storyModal')) closeStoryViewer();
  });

  el('closeHighlightBtn')?.addEventListener('click', closeHighlightViewer);
  el('highlightNextBtn')?.addEventListener('click', goToNextHighlightItem);
  el('highlightPrevBtn')?.addEventListener('click', goToPrevHighlightItem);
  el('highlightModal')?.addEventListener('click', (e) => {
    if (e.target === el('highlightModal')) closeHighlightViewer();
  });

  el('createHighlightBtn')?.addEventListener('click', async () => {
    setAlert(el('highlightsAlert'), '', '');
    const title = String(el('highlightTitleInput')?.value || '').trim();
    if (!title) return;
    try {
      await apiFetch('/highlights', { method: 'POST', body: JSON.stringify({ title }) });
      el('highlightTitleInput').value = '';
      await refreshMyHighlights();
      setAlert(el('highlightsAlert'), 'تم إنشاء Highlight ✅', 'ok');
    } catch (err) {
      setAlert(el('highlightsAlert'), `فشل الإنشاء: ${err.data?.error || err.message}`, 'danger');
    }
  });

  el('saveStoryToHighlightBtn')?.addEventListener('click', async () => {
    setAlert(el('storyCommentsAlert'), '', '');
    const s = getCurrentStory();
    if (!s?.id) return;
    try {
      if (!state.me) await loadMe();
      await refreshMyHighlights();
      const list = Array.isArray(state.myHighlights) ? state.myHighlights : [];
      if (!list.length) {
        setAlert(el('storyCommentsAlert'), 'أنشئ Highlight أولاً من صفحة البروفايل.', 'danger');
        return;
      }

      const choices = list.map((h, i) => `${i + 1}) ${h.title}`).join('\n');
      const pick = window.prompt(`اختر رقم Highlight لحفظ الستوري فيه:\n${choices}`);
      const idx = Number(pick);
      if (!Number.isFinite(idx) || idx < 1 || idx > list.length) return;
      const hid = list[idx - 1].id;

      await apiFetch(`/highlights/${encodeURIComponent(String(hid))}/items`, {
        method: 'POST',
        body: JSON.stringify({ storyId: String(s.id) }),
      });
      await refreshMyHighlights();
      setAlert(el('storyCommentsAlert'), 'تم الحفظ في Highlights ✅', 'ok');
    } catch (err) {
      setAlert(el('storyCommentsAlert'), `فشل الحفظ: ${err.data?.error || err.message}`, 'danger');
    }
  });

  el('storyCommentSendBtn')?.addEventListener('click', async () => {
    setAlert(el('storyCommentsAlert'), '', '');
    const s = getCurrentStory();
    if (!s?.id) return;
    const text = String(el('storyCommentInput')?.value || '').trim();
    if (!text) return;
    try {
      await apiFetch(`/stories/${encodeURIComponent(String(s.id))}/comments`, {
        method: 'POST',
        body: JSON.stringify({ text }),
      });
      el('storyCommentInput').value = '';
      await refreshStoryComments();
    } catch (err) {
      setAlert(el('storyCommentsAlert'), `فشل نشر التعليق: ${err.data?.error || err.message}`, 'danger');
    }
  });

  el('createPickSoundFileBtn')?.addEventListener('click', async (e) => {
    e.preventDefault();
    setAlert(el('createSoundAlert'), '', '');
    try {
      if (!state.me) await loadMe();
      if (!state.me?.verified) {
        setAlert(el('createSoundAlert'), 'هذه الميزة متاحة للحسابات الموثّقة فقط.', 'danger');
        return;
      }
      el('createSoundFile')?.click();
    } catch (err) {
      setAlert(el('createSoundAlert'), `فشل: ${err.data?.error || err.message}`, 'danger');
    }
  });

  el('createSoundFile')?.addEventListener('change', async () => {
    setAlert(el('createSoundAlert'), '', '');
    try {
      if (!state.me) await loadMe();
      if (!state.me?.verified) {
        setAlert(el('createSoundAlert'), 'هذه الميزة متاحة للحسابات الموثّقة فقط.', 'danger');
        return;
      }

      const file = el('createSoundFile')?.files?.[0];
      if (!file) return;
      const name = String(file.name || 'audio.mp3');
      const isMp3 = name.toLowerCase().endsWith('.mp3') || String(file.type || '').includes('mpeg');
      if (!isMp3) {
        setAlert(el('createSoundAlert'), 'اختر ملف mp3 فقط.', 'danger');
        el('createSoundFile').value = '';
        return;
      }

      const maxBytes = 20 * 1024 * 1024;
      if (Number(file.size || 0) > maxBytes) {
        setAlert(el('createSoundAlert'), 'الملف كبير جدًا. الحد الأقصى 20MB.', 'danger');
        el('createSoundFile').value = '';
        return;
      }

      setAlert(el('createSoundAlert'), 'جاري رفع الملف...', 'ok');
      const base64 = await new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onerror = () => reject(new Error('file_read_failed'));
        reader.onload = () => {
          try {
            const result = String(reader.result || '');
            const idx = result.indexOf('base64,');
            if (idx === -1) return reject(new Error('invalid_file_data'));
            resolve(result.slice(idx + 'base64,'.length));
          } catch (e) {
            reject(e);
          }
        };
        reader.readAsDataURL(file);
      });

      const data = await apiFetch('/sounds/upload', {
        method: 'POST',
        body: JSON.stringify({ filename: name, base64 }),
      });

      if (data?.url) {
        el('createSoundUrl').value = String(data.url);
        setAlert(el('createSoundAlert'), 'تم رفع الملف ✅', 'ok');
      } else {
        setAlert(el('createSoundAlert'), 'فشل رفع الملف.', 'danger');
      }
    } catch (err) {
      const reason = err.data?.error || err.message;
      const msg =
        reason === 'sound_upload_requires_verified'
          ? 'هذه الميزة متاحة للحسابات الموثّقة فقط.'
          : reason === 'only_mp3_allowed'
            ? 'اختر ملف mp3 فقط.'
            : reason === 'file_too_large'
              ? 'الملف كبير جدًا. الحد الأقصى 20MB.'
              : `فشل رفع الملف: ${reason}`;
      setAlert(el('createSoundAlert'), msg, 'danger');
    } finally {
      el('createSoundFile') && (el('createSoundFile').value = '');
    }
  });

  el('adminAddSoundBtn')?.addEventListener('click', async () => {
    setAlert(el('adminSoundsAlert'), '', '');
    try {
      const title = String(el('adminSoundTitle')?.value || '').trim();
      const url = String(el('adminSoundUrl')?.value || '').trim();
      if (!title || !url) {
        setAlert(el('adminSoundsAlert'), 'اكتب عنوان ورابط الصوت.', 'danger');
        return;
      }
      await apiFetch('/admin/sounds', { method: 'POST', body: JSON.stringify({ title, url }) });
      el('adminSoundTitle').value = '';
      el('adminSoundUrl').value = '';
      const data = await apiFetch('/sounds');
      state.adminSounds = Array.isArray(data.sounds) ? data.sounds : [];
      renderAdminSounds();
      setAlert(el('adminSoundsAlert'), 'تمت الإضافة ✅', 'ok');
    } catch (err) {
      setAlert(el('adminSoundsAlert'), `فشل الإضافة: ${err.data?.error || err.message}`, 'danger');
    }
  });

  el('aiSummarizeBtn').addEventListener('click', async () => {
    const text = el('postText').value;
    const data = await apiFetch('/ai/post/summarize', { method: 'POST', body: JSON.stringify({ text }) });
    state.aiDraft = data.summary;
    el('aiOutput').textContent = data.summary;
    el('applyAiToPostBtn').disabled = !state.aiDraft;
  });

  el('aiImproveBtn').addEventListener('click', async () => {
    const text = el('postText').value;
    const data = await apiFetch('/ai/post/improve', { method: 'POST', body: JSON.stringify({ text }) });
    state.aiDraft = data.improved;
    el('aiOutput').textContent = data.improved;
    el('applyAiToPostBtn').disabled = !state.aiDraft;
  });

  el('aiRewriteFormalBtn').addEventListener('click', async () => {
    const text = el('postText').value;
    const data = await apiFetch('/ai/post/rewrite', { method: 'POST', body: JSON.stringify({ text, style: 'formal' }) });
    state.aiDraft = data.rewritten;
    el('aiOutput').textContent = data.rewritten;
    el('applyAiToPostBtn').disabled = !state.aiDraft;
  });

  el('aiRewriteCasualBtn').addEventListener('click', async () => {
    const text = el('postText').value;
    const data = await apiFetch('/ai/post/rewrite', { method: 'POST', body: JSON.stringify({ text, style: 'casual' }) });
    state.aiDraft = data.rewritten;
    el('aiOutput').textContent = data.rewritten;
    el('applyAiToPostBtn').disabled = !state.aiDraft;
  });

  el('aiTranslateEnBtn').addEventListener('click', async () => {
    const text = el('postText').value;
    const data = await apiFetch('/ai/post/translate', {
      method: 'POST',
      body: JSON.stringify({ text, targetLang: 'en' }),
    });
    state.aiDraft = data.translated;
    el('aiOutput').textContent = data.translated;
    el('applyAiToPostBtn').disabled = !state.aiDraft;
  });

  el('applyAiToPostBtn').addEventListener('click', () => {
    if (!state.aiDraft) return;
    el('postText').value = state.aiDraft;
  });

  el('chatUserSearchBtn').addEventListener('click', async () => {
    setAlert(el('chatAlert'), '', '');
    try {
      const q = el('chatUserSearchInput').value.trim();
      if (!q) {
        el('chatUserSearchList').innerHTML = '';
        return;
      }
      const data = await apiFetch(`/users?q=${encodeURIComponent(q)}`);
      renderChatSearchUsers(data.users || []);
    } catch (err) {
      setAlert(el('chatAlert'), `فشل البحث: ${err.data?.error || err.message}`, 'danger');
    }
  });

  el('sendMessageBtn').addEventListener('click', async () => {
    setAlert(el('chatAlert'), '', '');
    try {
      const text = el('messageInput').value;
      if (!text.trim()) return;
      if (!state.selectedChatId) {
        setAlert(el('chatAlert'), 'اختر محادثة أولًا.', 'danger');
        return;
      }

      const chatId = state.selectedChatId;
      const isAi = el('chatTitle').textContent.includes('AI');
      if (isAi) await sendAiMessage(text);
      else await sendMessageToChat(chatId, text);

      el('messageInput').value = '';
      await openChat(chatId);
      await refreshChats();
    } catch (err) {
      setAlert(el('chatAlert'), `فشل الإرسال: ${err.data?.error || err.message}`, 'danger');
    }
  });

  el('saveProfileBtn').addEventListener('click', async () => {
    setAlert(el('profileAlert'), '', '');
    try {
      const payload = {
        username: el('editUsername').value,
        bio: el('editBio').value,
        avatarUrl: el('editAvatarUrl').value,
      };
      const data = await apiFetch('/me', { method: 'PATCH', body: JSON.stringify(payload) });
      state.me = data.user;
      await loadMe();
      setAlert(el('profileAlert'), 'تم حفظ البيانات ✅', 'ok');
    } catch (err) {
      setAlert(el('profileAlert'), `فشل الحفظ: ${err.data?.error || err.message}`, 'danger');
    }
  });

  el('otherFollowBtn').addEventListener('click', async () => {
    setAlert(el('otherAlert'), '', '');
    try {
      if (!state.otherUser?.id) return;
      await apiFetch(`/users/${encodeURIComponent(state.otherUser.id)}/follow`, { method: 'POST' });
      await loadMe();
      await loadOtherUser(state.otherUser.id);
      setAlert(el('otherAlert'), 'تمت المتابعة ✅', 'ok');
      await refreshNotifications();
    } catch (err) {
      setAlert(el('otherAlert'), `فشل: ${err.data?.error || err.message}`, 'danger');
    }
  });

  el('otherUnfollowBtn').addEventListener('click', async () => {
    setAlert(el('otherAlert'), '', '');
    try {
      if (!state.otherUser?.id) return;
      await apiFetch(`/users/${encodeURIComponent(state.otherUser.id)}/unfollow`, { method: 'POST' });
      await loadMe();
      await loadOtherUser(state.otherUser.id);
      setAlert(el('otherAlert'), 'تم إلغاء المتابعة ✅', 'ok');
      await refreshNotifications();
    } catch (err) {
      setAlert(el('otherAlert'), `فشل: ${err.data?.error || err.message}`, 'danger');
    }
  });
}

async function init() {
  wireEvents();
  setupAudioAutoplayUnlock();

  if (state.token) {
    try {
      setLoggedInUI(true);
      await loadMe();
      await refreshAdminStatus();

      if (window.location.pathname === '/dashbord') {
        if (state.isAdmin) {
          showView('dashboard');
          await refreshDashboard();
        } else {
          showView('feed');
          await refreshFeed();
        }
      } else {
        const mid = getMeetingIdFromUrl();
        if (mid) {
          showView('meetings');
          await openMeeting(mid);
        } else {
          showView('feed');
          await refreshFeed();
        }
      }
      await refreshNotifications();
      setChatPolling(true);
      startAppPolling();
      return;
    } catch {
      state.token = '';
      localStorage.removeItem('nc_token');
    }
  }

  setLoggedInUI(false);
  showView('auth');
  setAuthTab('login');
  setChatPolling(false);
  stopAppPolling();
}

init();
