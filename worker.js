export class GameRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.room = null;
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
      createdAt: now,
      updatedAt: now,
      bidDurationMs: 10000,
      choiceDurationMs: 10000,
      mainTimeMs: 300000
    };
  }

  async _save() {
    this.room.updatedAt = Date.now();
    await this.state.storage.put('room', this.room);
  }

  _now() {
    return Date.now();
  }

  async fetch(request) {
    await this._load();
    const url = new URL(request.url);
    const path = url.pathname.replace(/\/\/+/g, '/');
    try {
      if (path === '/initRoom' && request.method === 'POST') return this._handleInit(request);
      if (path === '/joinRoom' && request.method === 'POST') return this._handleJoin(request);
      if (path === '/startBidding' && request.method === 'POST') return this._handleStartBidding(request);
      if (path === '/submitBid' && request.method === 'POST') return this._handleSubmitBid(request);
      if (path === '/chooseColor' && request.method === 'POST') return this._handleChooseColor(request);
      if (path === '/makeMove' && request.method === 'POST') return this._handleMakeMove(request);
      if (path === '/getState' && request.method === 'GET') return this._handleGetState();
      return new Response(JSON.stringify({ error: 'not_found' }), { status: 404 });
    } catch (err) {
      return new Response(JSON.stringify({ error: err?.message || String(err) }), { status: 400 });
    }
  }

  async _handleInit(request) {
    if (this.room?.roomId) return new Response(JSON.stringify({ error: 'already_initialized' }), { status: 400 });
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
    return new Response(JSON.stringify({ ok: true, roomId: this.room.roomId }));
  }

  async _handleJoin(request) {
    const body = await request.json();
    const { playerId, name } = body;
    if (!playerId) return new Response(JSON.stringify({ error: 'playerId_required' }), { status: 400 });
    if (this.room.phase !== 'LOBBY') return new Response(JSON.stringify({ error: 'not_in_lobby' }), { status: 400 });
    if (this.room.players.find(p => p.id === playerId)) return new Response(JSON.stringify({ ok: true, room: this.room }));
    if (this.room.players.length >= this.room.maxPlayers) return new Response(JSON.stringify({ error: 'room_full' }), { status: 400 });
    this.room.players.push({ id: playerId, name: name || null, joinedAt: this._now() });
    await this._save();
    return new Response(JSON.stringify({ ok: true, room: this.room }));
  }

  async _handleStartBidding(request) {
    if (this.room.phase !== 'LOBBY') return new Response(JSON.stringify({ error: 'invalid_phase' }), { status: 400 });
    if (this.room.players.length !== this.room.maxPlayers) return new Response(JSON.stringify({ error: 'need_more_players' }), { status: 400 });
    const now = this._now();
    this.room.phase = 'BIDDING';
    this.room.bidDeadline = now + this.room.bidDurationMs;
    this.room.bids = {};
    this.room.winnerId = null;
    this.room.loserId = null;
    this.room.winningBidMs = null;
    this.room.losingBidMs = null;
    this.room.drawOddsSide = null;
    await this._save();
    return new Response(JSON.stringify({ ok: true, bidDeadline: this.room.bidDeadline }));
  }

  async _handleSubmitBid(request) {
    const body = await request.json();
    const { playerId, amount } = body;
    if (this.room.phase !== 'BIDDING') return new Response(JSON.stringify({ error: 'not_bidding' }), { status: 400 });
    if (!playerId || typeof amount !== 'number') return new Response(JSON.stringify({ error: 'playerId_and_amount_required' }), { status: 400 });
    if (!this.room.players.find(p => p.id === playerId)) return new Response(JSON.stringify({ error: 'unknown_player' }), { status: 400 });

    const now = this._now();
    if (this.room.bidDeadline && now > this.room.bidDeadline) {
      await this._resolveBidsIfNeeded();
      return new Response(JSON.stringify({ error: 'bidding_closed' }), { status: 400 });
    }
    if (this.room.bids[playerId]) return new Response(JSON.stringify({ error: 'already_bid' }), { status: 400 });

    this.room.bids[playerId] = { amount, submittedAt: now };
    await this._save();
    await this._resolveBidsIfNeeded();
    return new Response(JSON.stringify({ ok: true }));
  }

  async _resolveBidsIfNeeded() {
    const now = this._now();
    const players = this.room.players.map(p => p.id);
    const bothBid = players.every(id => !!this.room.bids[id]);
    const deadlinePassed = this.room.bidDeadline && now > this.room.bidDeadline;
    if (!bothBid && !deadlinePassed) return;

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
      this.room.winnerId = entries[0][0];
      this.room.loserId = entries.length > 1 ? entries[1][0] : players.find(p => p !== this.room.winnerId) || null;
      this.room.winningBidMs = entries[0][1].amount;
      this.room.losingBidMs = entries.length > 1 ? entries[1][1].amount : null;
    }

    this.room.drawOddsSide = this.room.winnerId;
    this.room.phase = 'COLOR_PICK';
    this.room.choiceDeadline = now + this.room.choiceDurationMs;
    await this._save();
  }

  async _handleChooseColor(request) {
    const body = await request.json();
    const { playerId, color } = body;
    if (this.room.phase !== 'COLOR_PICK') return new Response(JSON.stringify({ error: 'not_in_color_pick' }), { status: 400 });
    if (playerId !== this.room.winnerId) return new Response(JSON.stringify({ error: 'not_winner' }), { status: 400 });
    if (!['white', 'black'].includes(color)) return new Response(JSON.stringify({ error: 'invalid_color' }), { status: 400 });

    const now = this._now();
    if (this.room.choiceDeadline && now > this.room.choiceDeadline) return new Response(JSON.stringify({ error: 'choice_deadline_passed' }), { status: 400 });

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
    this.room.phase = 'PLAYING';
    await this._save();
    return new Response(JSON.stringify({ ok: true, clocks: this.room.clocks }));
  }

  async _handleMakeMove(request) {
    const body = await request.json();
    const { playerId, move } = body;
    if (this.room.phase !== 'PLAYING') return new Response(JSON.stringify({ error: 'not_playing' }), { status: 400 });

    const playerColor = this.room.colors[playerId];
    if (!playerColor) return new Response(JSON.stringify({ error: 'unknown_player_color' }), { status: 400 });
    if (playerColor !== this.room.clocks.turn) return new Response(JSON.stringify({ error: 'not_your_turn' }), { status: 400 });

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
      return new Response(JSON.stringify({ ok: true, result: 'time_forfeit', winnerId }));
    }

    this.room.moves.push({ by: playerId, move, at: now });
    this.room.clocks.lastTickAt = now;
    this.room.clocks.turn = this.room.clocks.turn === 'white' ? 'black' : 'white';
    await this._save();
    return new Response(JSON.stringify({ ok: true, clocks: this.room.clocks, moves: this.room.moves }));
  }

  async _handleGetState() {
    await this._resolveBidsIfNeeded();
    return new Response(JSON.stringify({ ok: true, room: this.room }));
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const segments = url.pathname.replace(/(^\/|\/$)/g, '').split('/');

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
        return new Response(JSON.stringify({ ok: true, roomId, meta: data }), { headers: { 'Content-Type': 'application/json' } });
      }

      if (segments[0] === 'rooms' && segments[1]) {
        const roomId = segments[1];
        const id = env.GAME_ROOMS.idFromName(roomId);
        const obj = env.GAME_ROOMS.get(id);

        if (segments.length === 2 && request.method === 'GET') return obj.fetch(new Request('https://do/getState'));
        if (segments.length === 3 && segments[2] === 'join' && request.method === 'POST') return obj.fetch(new Request('https://do/joinRoom', request));
        if (segments.length === 3 && segments[2] === 'start-bidding' && request.method === 'POST') return obj.fetch(new Request('https://do/startBidding', request));
        if (segments.length === 3 && segments[2] === 'submit-bid' && request.method === 'POST') return obj.fetch(new Request('https://do/submitBid', request));
        if (segments.length === 3 && segments[2] === 'choose-color' && request.method === 'POST') return obj.fetch(new Request('https://do/chooseColor', request));
        if (segments.length === 3 && segments[2] === 'move' && request.method === 'POST') return obj.fetch(new Request('https://do/makeMove', request));
      }

      return new Response(JSON.stringify({ error: 'route_not_found' }), { status: 404 });
    } catch (err) {
      return new Response(JSON.stringify({ error: err?.message || String(err) }), { status: 500 });
    }
  }
};