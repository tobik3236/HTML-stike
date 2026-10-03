// net.js — мультиплеер STRIKE 2 для GitHub Pages: без своего сервера, без аккаунтов.
// Транспорт: публичные MQTT-брокеры по WebSocket (mqtt.js). Подключаемся сразу к двум брокерам
// и дублируем сообщения (дубли отсекаются по номеру) — если один недоступен, игра идёт через второй.
// Интерфейс тот же, что игра ждёт от window.claude.use('room').
(() => {
  'use strict';
  const BROKERS = ['wss://broker.hivemq.com:8884/mqtt', 'wss://broker.emqx.io:8084/mqtt'];
  const NS = (() => { try { return (new URLSearchParams(location.search).get('ns') || 'pub').replace(/[^a-z0-9_-]/gi, '').slice(0, 24) || 'pub'; } catch (e) { return 'pub'; } })();
  const PREFIX = 'strike2/v1/' + NS + '/';
  const HEARTBEAT = 2000, EXPIRE = 7000, MAX_LEN = 2000, MAX_ROOMS = 3;
  const ROOM_RE = /^(lobby|s[a-z0-9]{3,12})$/;

  const myId = Math.random().toString(36).slice(2, 8) + Math.random().toString(36).slice(2, 4);
  const rooms = new Map(), lastN = new Map(), clients = [];
  let seq = 0, connP = null;

  const topic = r => PREFIX + r;
  function pub(room, obj) {
    obj.i = myId; obj.n = ++seq;
    const s = JSON.stringify(obj);
    for (const c of clients) if (c.connected) { try { c.publish(topic(room), s, { qos: 0 }); } catch (e) {} }
    return clients.some(c => c.connected);
  }

  class Room {
    constructor(name) {
      this.name = name; this.map = new Map(); this.cbs = []; this.closers = [];
      this.mine = {}; this.left = false; this.lastSend = 0; this.timer = null; this.nt = 0;
    }
    list() {
      const out = [];
      for (const [peer, e] of this.map) out.push({ peer, presence: e.d || {}, isMe: peer === myId, sameTab: peer === myId, kind: 'viewer', guest: false, by: null });
      return out;
    }
    get peers() { return this.list(); }
    onPeers(cb, onClose) { if (cb) this.cbs.push(cb); if (onClose) this.closers.push(onClose); }
    notify() { if (this.left) return; for (const f of this.cbs) { try { f(this); } catch (e) { console.error(e); } } }
    start() {
      this.map.set(myId, { d: this.mine, t: Date.now() });
      for (const c of clients) if (c.connected) try { c.subscribe(topic(this.name), { qos: 0 }); } catch (e) {}
      pub(this.name, { k: 'h' });
      this.timer = setInterval(() => this.tick(), 1000);
      this.notify();
    }
    tick() {
      const now = Date.now();
      if (now - this.lastSend >= HEARTBEAT) this.send();
      let ch = false;
      for (const [p, e] of this.map) if (p !== myId && now - e.t > EXPIRE) { this.map.delete(p); ch = true; }
      if (ch) this.notify();
    }
    send() { this.lastSend = Date.now(); pub(this.name, { k: 'p', d: this.mine }); }
    presence(d) {
      this.mine = d || {};
      const e = this.map.get(myId); if (e) { e.d = this.mine; e.t = Date.now(); }
      if (!this.left) this.send();
      return Promise.resolve();
    }
    leave() {
      if (this.left) return;
      pub(this.name, { k: 'x' });
      this.left = true; clearInterval(this.timer); rooms.delete(this.name);
      for (const c of clients) if (c.connected) try { c.unsubscribe(topic(this.name)); } catch (e) {}
      this.map.clear();
    }
    async join(slug) { // только у лобби
      if (!ROOM_RE.test(slug) || slug === 'lobby') throw new Error('bad room');
      await connect();
      if (rooms.size >= MAX_ROOMS + 1) throw new Error('too many rooms');
      let r = rooms.get(slug);
      if (r && !r.left) return r;
      r = new Room(slug); rooms.set(slug, r); r.start();
      return r;
    }
  }

  function onMsg(t, buf) {
    if (!t.startsWith(PREFIX)) return;
    const room = rooms.get(t.slice(PREFIX.length)); if (!room || room.left) return;
    let s = typeof buf === 'string' ? buf : (buf && buf.toString ? buf.toString() : ''); if (s.length > MAX_LEN) return;
    let m; try { m = JSON.parse(s); } catch (e) { return; }
    if (!m || typeof m.i !== 'string' || m.i === myId || m.i.length > 12 || typeof m.n !== 'number') return;
    const key = room.name + '|' + m.i, prev = lastN.get(key) || 0;
    if (m.n <= prev && prev - m.n < 1000) return;      // дубль от второго брокера или опоздавшее
    lastN.set(key, m.n);
    if (m.k === 'x') { if (room.map.delete(m.i)) { lastN.delete(key); room.notify(); } return; }
    if (m.k === 'h') {
      if (!room.map.has(m.i)) room.map.set(m.i, { d: {}, t: Date.now() });
      setTimeout(() => { if (!room.left) room.send(); }, Math.random() * 300); // новичку — наш статус
      room.notify(); return;
    }
    if (m.k === 'p' && m.d && typeof m.d === 'object' && !Array.isArray(m.d)) {
      room.map.set(m.i, { d: m.d, t: Date.now() }); room.notify();
    }
  }

  function connect() {
    if (connP) return connP;
    if (typeof mqtt === 'undefined' || !mqtt.connect) return Promise.reject(new Error('mqtt.js не загрузился'));
    connP = new Promise((res, rej) => {
      let done = false, fails = 0;
      const fin = ok => { if (done) return; done = true; ok ? res() : (connP = null, rej(new Error('нет связи с брокерами'))); };
      for (const url of BROKERS) {
        let c;
        try { c = mqtt.connect(url, { clientId: 'st2_' + myId + '_' + clients.length, clean: true, reconnectPeriod: 2500, connectTimeout: 8000, keepalive: 30 }); }
        catch (e) { if (++fails >= BROKERS.length) fin(false); continue; }
        clients.push(c);
        c.on('connect', () => {
          for (const r of rooms.values()) if (!r.left) {
            try { c.subscribe(topic(r.name), { qos: 0 }); } catch (e) {}
            if (done) pub(r.name, { k: 'h' });      // после переподключения — снова поздороваться
          }
          fin(true);
        });
        c.on('message', onMsg);
        c.on('error', () => { if (!done && ++fails >= BROKERS.length) fin(false); });
      }
      setTimeout(() => fin(false), 10000);
    });
    return connP;
  }

  const lobby = new Room('lobby');
  rooms.set('lobby', lobby);
  Object.defineProperty(lobby, 'peers', { value: () => lobby.list() }); // у лобби peers() — функция
  lobby.map.set(myId, { d: {}, t: Date.now() });

  window.claude = window.claude || {};
  const orig = window.claude.use ? window.claude.use.bind(window.claude) : null;
  window.claude.use = async function (name, ...rest) {
    if (name !== 'room') { if (orig) return orig(name, ...rest); throw new Error('unsupported: ' + name); }
    await connect();
    if (!lobby.timer) lobby.start();
    return lobby;
  };

  addEventListener('pagehide', () => { for (const r of rooms.values()) if (!r.left) pub(r.name, { k: 'x' }); });
  window.STRIKE_NET = { id: myId, ns: NS, brokers: () => clients.filter(c => c.connected).length };
})();
