import React, { useEffect, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { Participant, RemoteTrackPublication, Room, RoomEvent, Track } from 'livekit-client';
import './style.css';
import './host-claim.css';

type Role = 'host' | 'participant';
type Person = { id: string; nickname: string; role: Role; livekit_identity: string; connection_state?: string };
type Join = { participant_id: string; nickname: string; role: Role; livekit_url: string; livekit_token: string; session_token: string; resume_token: string; connection_generation: string; recording_state?: string };
type Recording = { id: string; status: string };
type ServerState = { participants: Person[]; recording: Recording | null; room_generation: number; has_host: boolean };
const apiBase = '/voice-chat/api';

const api = async <T,>(path: string, init?: RequestInit): Promise<T> => {
  const { headers, ...request } = init ?? {};
  const response = await fetch(`${apiBase}${path}`, {
    ...request,
    headers: { 'content-type': 'application/json', ...headers },
  });
  if (!response.ok) throw new Error((await response.json().catch(() => ({}))).error ?? response.statusText);
  if (response.status === 204) return undefined as T;
  return response.json() as Promise<T>;
};

function App() {
  const [nickname, setNickname] = useState('');
  const [session, setSession] = useState<Join | null>(null);
  const [room, setRoom] = useState<Room | null>(null);
  const [people, setPeople] = useState<Person[]>([]);
  const [active, setActive] = useState<Set<string>>(new Set());
  const [status, setStatus] = useState('未连接');
  const [muted, setMuted] = useState(false);
  const [recording, setRecording] = useState<Recording | null>(null);
  const [roomGeneration, setRoomGeneration] = useState(1);
  const [hasHost, setHasHost] = useState(true);
  const [hostPassword, setHostPassword] = useState('');
  const [playbackBlocked, setPlaybackBlocked] = useState(false);
  const [error, setError] = useState('');
  const audioElements = useRef(new Map<string, HTMLMediaElement>());
  const resumeStarted = useRef(false);

  const refreshState = async (current = session) => {
    const next = await api<ServerState>('/state', { headers: current ? { authorization: `Bearer ${current.session_token}` } : {} });
    setPeople(next.participants);
    setRecording(next.recording);
    setRoomGeneration(next.room_generation);
    setHasHost(next.has_host);
  };

  useEffect(() => {
    if (!room) return;
    const timer = window.setInterval(() => { void refreshState().catch(() => undefined); }, 3_000);
    return () => { window.clearInterval(timer); room.disconnect(); audioElements.current.forEach((element) => element.remove()); audioElements.current.clear(); };
  }, [room]);

  const syncRoom = (connectedRoom: Room, currentSession: Join) => {
    const local = connectedRoom.localParticipant;
    const present = [local, ...connectedRoom.remoteParticipants.values()];
    setPeople((previous) => present.map((participant: Participant) => previous.find((person) => person.livekit_identity === participant.identity) ?? {
      id: participant.identity,
      nickname: participant.name || participant.identity,
      role: participant.identity === `p_${currentSession.participant_id.replaceAll('-', '')}` && currentSession.role === 'host' ? 'host' : 'participant',
      livekit_identity: participant.identity,
    }));
    void refreshState(currentSession).catch(() => undefined);
  };

  const connect = async (joined: Join) => {
    const connectedRoom = new Room({ adaptiveStream: true, dynacast: true });
    const attachAudio = (track: Track, publication: RemoteTrackPublication) => {
      if (track.kind !== Track.Kind.Audio) return;
      const element = track.attach();
      element.autoplay = true;
      element.dataset.trackSid = publication.trackSid;
      document.body.appendChild(element);
      audioElements.current.set(publication.trackSid, element);
    };
    const detachAudio = (track: Track, publication: RemoteTrackPublication) => {
      track.detach().forEach((element) => element.remove());
      audioElements.current.get(publication.trackSid)?.remove();
      audioElements.current.delete(publication.trackSid);
    };
    connectedRoom
      .on(RoomEvent.ParticipantConnected, () => syncRoom(connectedRoom, joined))
      .on(RoomEvent.ParticipantDisconnected, () => syncRoom(connectedRoom, joined))
      .on(RoomEvent.TrackSubscribed, attachAudio)
      .on(RoomEvent.TrackUnsubscribed, detachAudio)
      .on(RoomEvent.ActiveSpeakersChanged, (speakers) => setActive(new Set(speakers.map((speaker) => speaker.identity))))
      .on(RoomEvent.Reconnecting, () => setStatus('重连中'))
      .on(RoomEvent.Reconnected, () => { setStatus('已连接'); syncRoom(connectedRoom, joined); })
      .on(RoomEvent.Disconnected, () => setStatus('已断开'))
      .on(RoomEvent.AudioPlaybackStatusChanged, () => setPlaybackBlocked(!connectedRoom.canPlaybackAudio));
    setStatus('连接中');
    await connectedRoom.connect(joined.livekit_url, joined.livekit_token);
    setSession(joined);
    setRoom(connectedRoom);
    try {
      await connectedRoom.localParticipant.setMicrophoneEnabled(true, {
        echoCancellation: true,
        noiseSuppression: true,
        autoGainControl: true,
        channelCount: 1,
      });
      setMuted(false);
    } catch {
      setMuted(true);
      setError('身份已恢复，请点击“恢复麦克风”继续发言。');
    }
    if (!connectedRoom.canPlaybackAudio) setPlaybackBlocked(true);
    setStatus('已连接');
    syncRoom(connectedRoom, joined);
  };

  const rememberSession = (joined: Join, automatic = true) => {
    localStorage.setItem('resume_token', joined.resume_token);
    localStorage.setItem('nickname', joined.nickname);
    localStorage.setItem('auto_resume', automatic ? '1' : '0');
    localStorage.removeItem('participant_id');
    localStorage.removeItem('host_token');
    setNickname(joined.nickname);
  };

  const resumeIdentity = async (automatic = false) => {
    const resumeToken = localStorage.getItem('resume_token');
    if (!resumeToken) return;
    try {
      setError('');
      setStatus('正在恢复');
      const joined = await api<Join>('/resume', { method: 'POST', body: JSON.stringify({ resume_token: resumeToken }) });
      rememberSession(joined, true);
      await connect(joined);
    } catch (cause) {
      setStatus('未连接');
      if (!automatic) setError((cause as Error).message || '无法恢复之前的身份');
    }
  };

  useEffect(() => {
    if (resumeStarted.current) return;
    resumeStarted.current = true;
    setNickname(localStorage.getItem('nickname') ?? '');
    if (localStorage.getItem('auto_resume') !== '0' && localStorage.getItem('resume_token')) {
      void resumeIdentity(true);
    }
  }, []);

  const join = async () => {
    try {
      setError('');
      const legacyHostToken = localStorage.getItem('host_token');
      const joined = await api<Join>('/join', {
        method: 'POST',
        headers: legacyHostToken ? { authorization: `Bearer ${legacyHostToken}` } : {},
        body: JSON.stringify({ nickname }),
      });
      rememberSession(joined, true);
      await connect(joined);
    } catch (cause) {
      setStatus('未连接');
      setError((cause as Error).message || '无法加入语音房间');
    }
  };

  const toggleMicrophone = async () => {
    if (!room) return;
    try {
      await room.localParticipant.setMicrophoneEnabled(muted);
      setMuted(!muted);
    } catch (cause) { setError((cause as Error).message); }
  };

  const leaveRoom = async () => {
    if (!session) return;
    try {
      await api<void>('/leave', {
        method: 'POST',
        headers: { authorization: `Bearer ${session.session_token}` },
      });
    } catch (cause) {
      setError((cause as Error).message || '无法确认退出状态');
    } finally {
      room?.disconnect();
      audioElements.current.forEach((element) => element.remove());
      audioElements.current.clear();
      setRoom(null);
      setSession(null);
      setPeople([]);
      setActive(new Set());
      setStatus('未连接');
      localStorage.setItem('auto_resume', '0');
    }
  };

  const enableAudio = async () => {
    if (!room) return;
    try { await room.startAudio(); setPlaybackBlocked(false); } catch (cause) { setError((cause as Error).message); }
  };

  const record = async (action: 'start' | 'stop') => {
    try {
      const path = action === 'start' ? '/recordings/start' : `/recordings/${recording?.id}/stop`;
      const result = await api<Recording>(path, { method: 'POST', headers: { authorization: `Bearer ${session?.session_token}` } });
      setRecording(result);
      await refreshState();
    } catch (cause) { setError((cause as Error).message); }
  };

  const claimHost = async () => {
    if (!session || !hostPassword) return;
    try {
      await api<{ role: Role }>('/host/claim', {
        method: 'POST',
        headers: { authorization: `Bearer ${session.session_token}` },
        body: JSON.stringify({ password: hostPassword }),
      });
      const hostedSession = { ...session, role: 'host' as Role };
      setSession(hostedSession);
      setHasHost(true);
      setHostPassword('');
      await refreshState(hostedSession);
    } catch (cause) {
      setError((cause as Error).message || '无法认领房主');
    }
  };

  if (!session) return <main className="join"><section><p className="eyebrow">MAIN ROOM / LIVE AUDIO</p><h1>进入语音房间</h1><p className="sub">固定房间，支持 2-8 人同时通话。</p><label>昵称<input value={nickname} onChange={(event) => setNickname(event.target.value)} maxLength={80} placeholder="输入昵称" /></label><div className="actions">{localStorage.getItem('resume_token') && <button onClick={() => void resumeIdentity(false)}>以上次身份加入</button>}<button className="secondary" onClick={() => void join()} disabled={!nickname.trim()}>以新身份加入</button></div><p className="hint">刷新页面会自动恢复当前身份；只有点击“退出房间”才会结束本次在线状态。</p>{error && <p className="error">{error}</p>}</section></main>;
  return <main className="room"><header><div><p className="eyebrow">MAIN ROOM / SESSION {roomGeneration}</p><h1>语音聊天室</h1></div><span className={`status ${status === '已连接' ? 'live' : ''}`}>{status}</span></header><section className="toolbar"><button onClick={() => void toggleMicrophone()}>{muted ? '恢复麦克风' : '静音麦克风'}</button>{playbackBlocked && <button className="secondary" onClick={() => void enableAudio()}>启用声音</button>}{session.role === 'host' && <><button onClick={() => void record('start')} disabled={recording?.status === 'starting' || recording?.status === 'recording'}>开始录音</button><button className="secondary" onClick={() => void record('stop')} disabled={!recording || recording.status === 'completed' || recording.status === 'stopping'}>结束录音</button></>}<button className="secondary" onClick={() => void leaveRoom()}>退出房间</button>{recording && <span className="recording">录音状态：{recording.status}</span>}</section>{!hasHost && <section className="host-claim"><input type="password" value={hostPassword} onChange={(event) => setHostPassword(event.target.value)} placeholder="房主口令" autoComplete="current-password"/><button onClick={() => void claimHost()} disabled={!hostPassword}>成为房主</button></section>}<section className="participants"><h2>参与者 <span>{people.length}/8</span></h2>{people.map((person) => <article key={person.id}><div className="avatar">{person.nickname.slice(0, 1).toUpperCase()}</div><div className="person"><strong>{person.nickname}</strong>{person.role === 'host' && <small>主持人</small>}<div className="meter"><i style={{ width: person.livekit_identity === `p_${session.participant_id.replaceAll('-', '')}` && muted ? '0%' : active.has(person.livekit_identity) ? '88%' : '8%' }} /></div></div><span className="mic-state">{person.connection_state === 'reconnecting' ? '重连中' : person.livekit_identity === `p_${session.participant_id.replaceAll('-', '')}` && muted ? '已静音' : '在线'}</span></article>)}</section>{error && <p className="error">{error}</p>}</main>;
}

createRoot(document.getElementById('root')!).render(<React.StrictMode><App /></React.StrictMode>);
