import { Chess } from 'chess.js';

export class GameRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.room = null;
    this._sockets = new Set();
  }

  _corsHeaders() {
    return {
      "Access-Control-Allow-Origin": (this.env && this.env.FRONTEND_URL) || "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
    };
  }

  _response(body = null, status = 200, extra = {}) {
    const headers = Object.assign({}, this._corsHeaders(), extra);
    if (status === 204) return new Response(null, { status, headers });
    return new Response(JSON.stringify(body), { status, headers });
  }

  async _load() {
    if (this.room) return;
    const stored = await this.state.storage.get('room');
    if (stored) this.room = stored;
    else this.room = this._defaultState();
  }

  _defaultState() {
    const now = Date.now();
    return {
      roomId: null,
      phase: 'LOBBY',
      players: [],
      maxPlayers: 2,
      bids: {},
      bidDeadline: null,
      choiceDeadline: null,
      winnerId: null,
      loserId: null,
      winningBidMs: null,
      losingBidMs: null,
      drawOddsSide: null,
      colors: {},
      clocks: null,
      moves: [],
      choiceAttempts: 0,
      currentPicker: null,
      createdAt: now,
      updatedAt: now,
      bidDurationMs: 10000,
      choiceDurationMs: 10000,
      mainTimeMs: 300000
    };
  }

  // Helper: notify RoomIndex about room metadata
  async _indexUpdate() {
    try {
      if (!this.env || !this.env.ROOM_INDEX) return;
      const indexId = this.env.ROOM_INDEX.idFromName('index');
      const obj = this.env.ROOM_INDEX.get(indexId);
      const meta = {
        roomId: this.room.roomId,
        phase: this.room.phase,
        players: (this.room.players || []).length,
        updatedAt: this.room.updatedAt || this._now()
      };
      await obj.fetch(new Request('https://do/update', { method: 'POST', body: JSON.stringify(meta), headers: { 'Content-Type': 'application/json' } }));
    } catch (e) {}
  }

  async _save() {
    this.room.updatedAt = Date.now();
    await this.state.storage.put('room', this.room);
    this._broadcastUpdate();
    await this._indexUpdate();
  }

  _now() {
    return Date.now();
  }

  async fetch(request) {
    await this._load();
    if (request.headers.get('Upgrade') === 'websocket') {
      return this._handleWebSocket(request);
    }
    if (request.method === 'OPTIONS') return this._response(null, 204);
    const url = new URL(request.url);
    const path = url.pathname.replace(/\/\/+/g, '/');
    try {
      if (path === '/initRoom' && request.method === 'POST') return this._handleInit(request);
      if (path === '/joinRoom' && request.method === 'POST') return this._handleJoin(request);
      if (path === '/startBidding' && request.method === 'POST') return this._handleStartBidding(request);
      if (path === '/submitBid' && request.method === 'POST') return this._handleSubmitBid(request);
      if (path === '/chooseColor' && request.method === 'POST') return this._handleChooseColor(request);
      if (path === '/makeMove' && request.method === 'POST') return this._handleMakeMove(request);
      if (path === '/rematch' && request.method === 'POST') return this._handleRematch(request);
      if (path === '/getState' && request.method === 'GET') return this._handleGetState();
      return this._response({ error: 'not_found' }, 404);
    } catch (err) {
      return this._response({ error: err?.message || String(err) }, 400);
    }
  }

  _handleWebSocket(request) {
    const url = new URL(request.url);
    const playerId = url.searchParams.get('playerId');
    if (!playerId) {
      return this._response({ error: 'playerId_required' }, 400);
    }

    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);

    server.accept();
    server.serializeAttachment({ playerId });  // Store playerId for identification on wake-up

    // track this server socket for broadcasts while the DO instance is alive
    try {
      this._sockets.add(server);
      server.addEventListener('close', () => {
        try { this._sockets.delete(server); } catch (e) {}
      });
    } catch (e) {}

    // Send initial state immediately
    server.send(JSON.stringify({ type: 'init', room: this.room }));

    // No need for manual close listener - runtime handles hibernation
    return new Response(null, { status: 101, webSocket: client });
  }

  _broadcastUpdate() {
    const msg = JSON.stringify({ type: 'update', room: this.room });
    for (const ws of this._sockets) {
      try {
        if (ws && ws.readyState === 1) ws.send(msg);
      } catch (e) {
        // ignore
      }
    }
  }

  async _handleInit(request) {
    if (this.room?.roomId) return this._response({ error: 'already_initialized' }, 400);
    const body = await request.json();
    const now = this._now();
    this.room.roomId = body.roomId || `room-${crypto.randomUUID()}`;
    this.room.maxPlayers = body.maxPlayers || 2;
    this.room.bidDurationMs = body.bidDurationMs || this.room.bidDurationMs;
    this.room.choiceDurationMs = body.choiceDurationMs || this.room.choiceDurationMs;
    this.room.mainTimeMs = body.mainTimeMs || this.room.mainTimeMs;
    this.room.createdAt = now;
    this.room.phase = 'LOBBY';
    await this._save();
    return this._response({ ok: true, roomId: this.room.roomId });
  }

  async _handleJoin(request) {
    const body = await request.json();
    const { playerId, name } = body;
    if (!playerId) return this._response({ error: 'playerId_required' }, 400);
    if (this.room.phase !== 'LOBBY') return this._response({ error: 'not_in_lobby' }, 400);
    if (this.room.players.find(p => p.id === playerId)) return this._response({ ok: true, room: this.room });
    if (this.room.players.length >= this.room.maxPlayers) return this._response({ error: 'room_full' }, 400);
    this.room.players.push({ id: playerId, name: name || null, joinedAt: this._now() });
    await this._save();
    return this._response({ ok: true, room: this.room });
  }

  async _handleStartBidding(request) {
    // Two-step start: first press requests bidding start, other player must confirm within choiceDurationMs (default 60s)
    const body = await request.json().catch(() => ({}));
    const { playerId } = body;
    if (!playerId) return this._response({ error: 'playerId_required' }, 400);
    if (this.room.phase !== 'LOBBY') return this._response({ error: 'invalid_phase' }, 400);
    if (this.room.players.length !== this.room.maxPlayers) return this._response({ error: 'need_more_players' }, 400);
    const now = this._now();

    // if there's already a pending start request
    if (this.room.startRequestedBy && this.room.startConfirmDeadline) {
      // if same player presses again, ignore
      if (this.room.startRequestedBy === playerId) return this._response({ ok: true, message: 'already_requested' });
      // if within deadline, begin bidding
      if (now <= this.room.startConfirmDeadline) {
        this.room.phase = 'BIDDING';
        this.room.bidDeadline = now + this.room.bidDurationMs;
        this.room.bids = {};
        this.room.winnerId = null;
        this.room.loserId = null;
        this.room.winningBidMs = null;
        this.room.losingBidMs = null;
        this.room.drawOddsSide = null;
        this.room.choiceAttempts = 0;
        this.room.currentPicker = null;
        // clear start request
        this.room.startRequestedBy = null;
        this.room.startConfirmDeadline = null;
        await this._save();
        return this._response({ ok: true, bidDeadline: this.room.bidDeadline });
      }
      // deadline passed - clear and revert
      this.room.startRequestedBy = null;
      this.room.startConfirmDeadline = null;
      await this._save();
      return this._response({ error: 'start_request_expired' }, 400);
    }

    // create a start request and set a confirm deadline
    this.room.startRequestedBy = playerId;
    this.room.startConfirmDeadline = now + (this.room.choiceDurationMs || 60 * 1000);
    await this._save();
    return this._response({ ok: true, startRequestedBy: playerId, startConfirmDeadline: this.room.startConfirmDeadline });
  }

  async _handleSubmitBid(request) {
    const body = await request.json();
    const { playerId, amount } = body;
    if (this.room.phase !== 'BIDDING') return this._response({ error: 'not_bidding' }, 400);
    if (!playerId || typeof amount !== 'number') return this._response({ error: 'playerId_and_amount_required' }, 400);
    if (!this.room.players.find(p => p.id === playerId)) return this._response({ error: 'unknown_player' }, 400);

    if (typeof this.room.mainTimeMs === 'number' && (amount < 0 || amount > this.room.mainTimeMs)) {
      return this._response({ error: 'invalid_bid_amount' }, 400);
    }

    const now = this._now();
    if (this.room.bidDeadline && now > this.room.bidDeadline) {
      await this._resolveBidsIfNeeded();
      return this._response({ error: 'bidding_closed' }, 400);
    }
    if (this.room.bids[playerId]) return this._response({ error: 'already_bid' }, 400);

    this.room.bids[playerId] = { amount, submittedAt: now };
    await this._save();
    await this._resolveBidsIfNeeded();
    return this._response({ ok: true });
  }

  async _resolveBidsIfNeeded() {
    if (this.room.phase !== 'BIDDING') return;
    const now = this._now();
    const players = this.room.players.map(p => p.id);
    const bothBid = players.every(id => !!this.room.bids[id]);
    const deadlinePassed = this.room.bidDeadline && now > this.room.bidDeadline;
    if (!bothBid && !deadlinePassed) return;

    if (deadlinePassed) {
      for (const pid of players) {
        if (!this.room.bids[pid]) {
          this.room.bids[pid] = { amount: this.room.mainTimeMs, submittedAt: now };
        }
      }
    }

    const entries = Object.entries(this.room.bids || {});
    if (entries.length === 0) {
      this.room.winnerId = players[0] || null;
      this.room.loserId = players.find(p => p !== this.room.winnerId) || null;
      this.room.winningBidMs = null;
      this.room.losingBidMs = null;
    } else {
      entries.sort(([aid, aobj], [bid, bobj]) => {
        if (aobj.amount !== bobj.amount) return aobj.amount - bobj.amount;
        if (aobj.submittedAt !== bobj.submittedAt) return aobj.submittedAt - bobj.submittedAt;
        return aid.localeCompare(bid);
      });

      if (entries.length > 1 && entries[0][1].amount === entries[1][1].amount) {
        this.room.bids = {};
        this.room.bidDeadline = now + this.room.bidDurationMs;
        await this._save();
        return;
      }

      this.room.winnerId = entries[0][0];
      this.room.loserId = entries.length > 1 ? entries[1][0] : players.find(p => p !== this.room.winnerId) || null;
      this.room.winningBidMs = entries[0][1].amount;
      this.room.losingBidMs = entries.length > 1 ? entries[1][1].amount : null;
    }

    this.room.drawOddsSide = null;
    this.room.phase = 'COLOR_PICK';
    this.room.choiceAttempts = 0;
    this.room.currentPicker = 'winner';
    this.room.choiceDeadline = now + this.room.choiceDurationMs;
    await this._save();
  }

  async _handleChooseColor(request) {
    const body = await request.json();
    const { playerId, color } = body;
    if (this.room.phase !== 'COLOR_PICK') return this._response({ error: 'not_in_color_pick' }, 400);
    const now = this._now();
    const allowedRole = this.room.currentPicker === 'winner' ? this.room.winnerId : this.room.loserId;
    if (playerId !== allowedRole) return this._response({ error: 'not_allowed_to_choose' }, 400);
    if (!['white', 'black'].includes(color)) return this._response({ error: 'invalid_color' }, 400);

    if (this.room.choiceDeadline && now > this.room.choiceDeadline) return this._response({ error: 'choice_deadline_passed' }, 400);

    const other = this.room.players.find(p => p.id !== playerId)?.id || null;
    this.room.colors = { [playerId]: color };
    if (other) this.room.colors[other] = color === 'white' ? 'black' : 'white';

    const winnerColor = color;
    const loserColor = winnerColor === 'white' ? 'black' : 'white';
    const winnerMs = typeof this.room.winningBidMs === 'number' ? this.room.winningBidMs : 0;
    const loserMs = this.room.mainTimeMs;

    this.room.clocks = {
      whiteRemainingMs: winnerColor === 'white' ? winnerMs : loserMs,
      blackRemainingMs: winnerColor === 'black' ? winnerMs : loserMs,
      lastTickAt: now,
      turn: 'white'
    };
    const blackPlayerId = Object.keys(this.room.colors).find(id => this.room.colors[id] === 'black') || null;
    this.room.drawOddsSide = blackPlayerId;
    this.room.phase = 'PLAYING';
    await this._save();
    return this._response({ ok: true, clocks: this.room.clocks });
  }

  async _resolveChoiceIfNeeded() {
    if (this.room.phase !== 'COLOR_PICK') return;
    const now = this._now();
    if (!this.room.choiceDeadline || now <= this.room.choiceDeadline) return;

    this.room.choiceAttempts = (this.room.choiceAttempts || 0) + 1;
    if (this.room.choiceAttempts >= 4) {
      this.room.phase = 'FINISHED';
      this.room.winnerId = null;
      await this._save();
      return;
    }

    this.room.currentPicker = this.room.currentPicker === 'winner' ? 'loser' : 'winner';
    this.room.choiceDeadline = now + this.room.choiceDurationMs;
    await this._save();
  }

  async _handleMakeMove(request) {
    const body = await request.json();
    const { playerId, move } = body;
    if (this.room.phase !== 'PLAYING') return this._response({ error: 'not_playing' }, 400);

    const playerColor = this.room.colors[playerId];
    if (!playerColor) return this._response({ error: 'unknown_player_color' }, 400);
    if (playerColor !== this.room.clocks.turn) return this._response({ error: 'not_your_turn' }, 400);

    const now = this._now();
    const elapsed = now - (this.room.clocks.lastTickAt || now);
    if (playerColor === 'white') this.room.clocks.whiteRemainingMs -= elapsed;
    else this.room.clocks.blackRemainingMs -= elapsed;

    if ((playerColor === 'white' && this.room.clocks.whiteRemainingMs <= 0) ||
        (playerColor === 'black' && this.room.clocks.blackRemainingMs <= 0)) {
      const winnerId = this.room.players.find(p => this.room.colors[p.id] !== playerColor)?.id || null;
      this.room.phase = 'FINISHED';
      this.room.winnerId = winnerId;
      await this._save();
      return this._response({ ok: true, result: 'time_forfeit', winnerId });
    }

    const game = new Chess();
    if (this.room.moves && this.room.moves.length > 0) {
      for (const m of this.room.moves) {
        try {
          if (typeof m.move === 'string' && m.move.length >= 4) {
            const from = m.move.slice(0,2);
            const to = m.move.slice(2,4);
            const promotion = m.move.length >= 5 ? m.move[4] : undefined;
            if (promotion) game.move({ from, to, promotion });
            else game.move({ from, to });
          } else {
            game.move(m.move);
          }
        } catch (e) {}
      }
    }

    if (typeof move !== 'string' || move.length < 4) return this._response({ error: 'invalid_move_format' }, 400);
    const from = move.slice(0,2);
    const to = move.slice(2,4);
    const promotion = move.length >= 5 ? move[4] : undefined;

    const test = new Chess(game.fen());
    const moved = test.move({ from, to, promotion: promotion || 'q' });
    if (!moved) return this._response({ error: 'illegal_move' }, 400);

    this.room.moves.push({ by: playerId, move, at: now });
    this.room.clocks.lastTickAt = now;
    this.room.clocks.turn = this.room.clocks.turn === 'white' ? 'black' : 'white';

    const after = new Chess(game.fen());
    after.move({ from, to, promotion: promotion || 'q' });
    const isCheckmate = (after.isCheckmate && typeof after.isCheckmate === 'function' && after.isCheckmate()) ||
                        (after.in_checkmate && typeof after.in_checkmate === 'function' && after.in_checkmate());
    if (isCheckmate) {
      this.room.phase = 'FINISHED';
      this.room.winnerId = playerId;
      // open rematch window (60s)
      this.room.rematchWindowEnds = this._now() + 60 * 1000;
      this.room.rematchVotes = {};
      await this._save();
      return this._response({ ok: true, result: 'checkmate', winnerId: playerId, clocks: this.room.clocks, moves: this.room.moves, rematchWindowEnds: this.room.rematchWindowEnds });
    }

    await this._save();
    return this._response({ ok: true, clocks: this.room.clocks, moves: this.room.moves });
  }

  async _handleGetState() {
    await this._resolveBidsIfNeeded();
    await this._resolveChoiceIfNeeded();
    // expire pending start-bidding requests
    const now = this._now();
    if (this.room.startConfirmDeadline && now > this.room.startConfirmDeadline) {
      this.room.startRequestedBy = null;
      this.room.startConfirmDeadline = null;
      this.room.phase = 'LOBBY';
      this.room.closed = true;
      await this._save();
    }
    // cleanup rematch window expiry and possibly mark room removable
    if (this.room.phase === 'FINISHED' && this.room.rematchWindowEnds && this._now() > this.room.rematchWindowEnds) {
      // if rematchVotes do not indicate unanimous agreement, remove index entry
      const players = (this.room.players || []).map(p => p.id);
      const votes = this.room.rematchVotes || {};
      const bothAgreed = players.length > 0 && players.every(pid => votes[pid] === true);
      if (!bothAgreed) {
        try {
          if (this.env && this.env.ROOM_INDEX) {
            const indexId = this.env.ROOM_INDEX.idFromName('index');
            const obj = this.env.ROOM_INDEX.get(indexId);
            await obj.fetch(new Request('https://do/remove', { method: 'POST', body: JSON.stringify({ roomId: this.room.roomId }), headers: { 'Content-Type': 'application/json' } }));
          }
        } catch (e) {}
      }
    }
    return this._response({ ok: true, room: this.room });
  }

  async _handleRematch(request) {
    const body = await request.json().catch(() => ({}));
    const { playerId, agree } = body;
    if (!playerId) return this._response({ error: 'playerId_required' }, 400);
    if (this.room.phase !== 'FINISHED') return this._response({ error: 'not_finished' }, 400);
    if (!this.room.rematchWindowEnds || this._now() > this.room.rematchWindowEnds) return this._response({ error: 'rematch_window_closed' }, 400);

    this.room.rematchVotes = this.room.rematchVotes || {};
    this.room.rematchVotes[playerId] = !!agree;
    await this._save();

    const players = (this.room.players || []).map(p => p.id);
    const allAgreed = players.length > 0 && players.every(pid => this.room.rematchVotes[pid] === true);
    if (allAgreed) {
      // reset to lobby for re-bidding
      this.room.phase = 'LOBBY';
      this.room.bids = {};
      this.room.bidDeadline = null;
      this.room.choiceDeadline = null;
      this.room.winnerId = null;
      this.room.loserId = null;
      this.room.winningBidMs = null;
      this.room.losingBidMs = null;
      this.room.drawOddsSide = null;
      this.room.colors = {};
      this.room.clocks = null;
      this.room.moves = [];
      this.room.choiceAttempts = 0;
      this.room.currentPicker = null;
      this.room.rematchWindowEnds = null;
      this.room.rematchVotes = null;
      await this._save();
      return this._response({ ok: true, rematchStarted: true, room: this.room });
    }

    return this._response({ ok: true, rematchStarted: false, votes: this.room.rematchVotes });
  }
}

// Durable Object to track active rooms for matchmaking
export class RoomIndex {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

  async _getAll() {
    const raw = await this.state.storage.get('rooms');
    return raw || {};
  }

  async _saveAll(obj) {
    await this.state.storage.put('rooms', obj);
  }

  async fetch(request) {
    if (request.method === 'POST' && request.url.endsWith('/update')) {
      const body = await request.json().catch(() => ({}));
      const rooms = await this._getAll();
      rooms[body.roomId] = { roomId: body.roomId, phase: body.phase, players: body.players || 0, updatedAt: body.updatedAt || Date.now() };
      await this._saveAll(rooms);
      return new Response(JSON.stringify({ ok: true }), { headers: { 'Content-Type': 'application/json' } });
    }
    if (request.method === 'POST' && request.url.endsWith('/remove')) {
      const body = await request.json().catch(() => ({}));
      const rooms = await this._getAll();
      delete rooms[body.roomId];
      await this._saveAll(rooms);
      return new Response(JSON.stringify({ ok: true }), { headers: { 'Content-Type': 'application/json' } });
    }
    if (request.method === 'GET' && request.url.endsWith('/list')) {
      const rooms = await this._getAll();
      const list = Object.values(rooms).filter(r => r.phase !== 'FINISHED');
      return new Response(JSON.stringify({ ok: true, rooms: list }), { headers: { 'Content-Type': 'application/json' } });
    }
    return new Response(JSON.stringify({ error: 'not_found' }), { status: 404, headers: { 'Content-Type': 'application/json' } });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const segments = url.pathname.replace(/(^\/|\/$)/g, '').split('/');
    const corsHeaders = {
      'Access-Control-Allow-Origin': env && env.FRONTEND_URL ? env.FRONTEND_URL : '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };
    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: corsHeaders });

    try {
      if (request.method === 'POST' && url.pathname === '/rooms') {
        const body = await request.json().catch(() => ({}));
        const roomId = body.roomId || `room-${crypto.randomUUID()}`;
        const id = env.GAME_ROOMS.idFromName(roomId);
        const obj = env.GAME_ROOMS.get(id);
        const initReq = new Request('https://do/initRoom', {
          method: 'POST',
          body: JSON.stringify({
            roomId,
            maxPlayers: body.maxPlayers,
            bidDurationMs: body.bidDurationMs,
            choiceDurationMs: body.choiceDurationMs,
            mainTimeMs: body.mainTimeMs
          }),
          headers: { 'Content-Type': 'application/json' }
        });
        const res = await obj.fetch(initReq);
        const data = await res.json();
        return new Response(JSON.stringify({ ok: true, roomId, meta: data }), { headers: Object.assign({ 'Content-Type': 'application/json' }, corsHeaders) });
      }

      // Join-next: choose a random lobby room via RoomIndex
      if (request.method === 'POST' && url.pathname === '/rooms/join-next') {
        const body = await request.json().catch(() => ({}));
        const playerId = body.playerId;
        const name = body.name || null;
        // query RoomIndex
        if (!env.ROOM_INDEX) return new Response(JSON.stringify({ error: 'no_matchmaking' }), { status: 500, headers: corsHeaders });
        const idxId = env.ROOM_INDEX.idFromName('index');
        const idxObj = env.ROOM_INDEX.get(idxId);
        const listRes = await idxObj.fetch(new Request('https://do/list'));
        const listData = await listRes.json().catch(() => ({ rooms: [] }));
        const rooms = listData.rooms || [];
        // filter lobby rooms with space
        const candidates = [];
        for (const r of rooms) {
          // fetch room state
          const id = env.GAME_ROOMS.idFromName(r.roomId);
          const obj = env.GAME_ROOMS.get(id);
          const stateRes = await obj.fetch(new Request('https://do/getState'));
          const data = await stateRes.json().catch(() => ({}));
          const room = data.room || data;
          if (room && room.phase === 'LOBBY' && (room.players || []).length < (room.maxPlayers || 2)) candidates.push(room);
        }
        if (candidates.length === 0) return new Response(JSON.stringify({ error: 'no_lobby_rooms' }), { status: 404, headers: corsHeaders });
        const pick = candidates[Math.floor(Math.random() * candidates.length)];
        // join that room via its DO
        const roomId = pick.roomId;
        const objId = env.GAME_ROOMS.idFromName(roomId);
        const obj = env.GAME_ROOMS.get(objId);
        const joinReq = new Request('https://do/joinRoom', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ playerId, name }) });
        const joinRes = await obj.fetch(joinReq);
        const joinData = await joinRes.json().catch(() => ({}));
        return new Response(JSON.stringify({ ok: true, room: joinData.room || joinData }), { headers: Object.assign({ 'Content-Type': 'application/json' }, corsHeaders) });
      }

      if (segments[0] === 'rooms' && segments[1]) {
        const roomId = segments[1];
        const id = env.GAME_ROOMS.idFromName(roomId);
        const obj = env.GAME_ROOMS.get(id);

        if (url.pathname.endsWith('/ws') && request.headers.get('Upgrade') === 'websocket') {
          return obj.fetch(request);
        }

        if (segments.length === 2 && request.method === 'GET') return obj.fetch(new Request('https://do/getState'));
        if (segments.length === 3 && segments[2] === 'join' && request.method === 'POST') {
          const bodyText = await request.clone().text().catch(() => null);
          const headers = { 'Content-Type': request.headers.get('Content-Type') || 'application/json' };
          return obj.fetch(new Request('https://do/joinRoom', { method: 'POST', headers, body: bodyText }));
        }
        if (segments.length === 3 && segments[2] === 'start-bidding' && request.method === 'POST') {
          const bodyText = await request.clone().text().catch(() => null);
          const headers = { 'Content-Type': request.headers.get('Content-Type') || 'application/json' };
          return obj.fetch(new Request('https://do/startBidding', { method: 'POST', headers, body: bodyText }));
        }
        if (segments.length === 3 && segments[2] === 'submit-bid' && request.method === 'POST') {
          const bodyText = await request.clone().text().catch(() => null);
          const headers = { 'Content-Type': request.headers.get('Content-Type') || 'application/json' };
          return obj.fetch(new Request('https://do/submitBid', { method: 'POST', headers, body: bodyText }));
        }
        if (segments.length === 3 && segments[2] === 'choose-color' && request.method === 'POST') {
          const bodyText = await request.clone().text().catch(() => null);
          const headers = { 'Content-Type': request.headers.get('Content-Type') || 'application/json' };
          return obj.fetch(new Request('https://do/chooseColor', { method: 'POST', headers, body: bodyText }));
        }
        if (segments.length === 3 && segments[2] === 'move' && request.method === 'POST') {
          const bodyText = await request.clone().text().catch(() => null);
          const headers = { 'Content-Type': request.headers.get('Content-Type') || 'application/json' };
          return obj.fetch(new Request('https://do/makeMove', { method: 'POST', headers, body: bodyText }));
        }
        if (segments.length === 3 && segments[2] === 'rematch' && request.method === 'POST') {
          const bodyText = await request.clone().text().catch(() => null);
          const headers = { 'Content-Type': request.headers.get('Content-Type') || 'application/json' };
          return obj.fetch(new Request('https://do/rematch', { method: 'POST', headers, body: bodyText }));
        }
      }
      return new Response(JSON.stringify({ error: 'route_not_found' }), { status: 404, headers: corsHeaders });
    } catch (err) {
      return new Response(JSON.stringify({ error: err?.message || String(err) }), { status: 500, headers: corsHeaders });
    }
  }
};