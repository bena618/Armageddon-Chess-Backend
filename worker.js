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
      mainTimeMs: 300000,
      private: false,
      
      closed: false,
      closeReason: null,
      closedAt: null,

      disconnectedPlayerId: null,
      disconnectStart: null,
      disconnectTimeoutMs: 45000,
    };
  }

  async _indexUpdate() {
    if (this.room.closed) return;

    try {
      if (!this.env || !this.env.ROOM_INDEX) return;
      const indexId = this.env.ROOM_INDEX.idFromName('index');
      const obj = this.env.ROOM_INDEX.get(indexId);
      const meta = {
        roomId: this.room.roomId,
        phase: this.room.phase,
        players: this.room.players || [],
        private: this.room.private || false,
        updatedAt: this.room.updatedAt || this._now(),
        clocks: this.room.clocks,
        mainTimeMs: this.room.mainTimeMs,
        liveWhiteMs: this.room.clocks?.whiteRemainingMs,
        liveBlackMs: this.room.clocks?.blackRemainingMs
      };
      await obj.fetch(new Request('https://do/update', {
        method: 'POST',
        body: JSON.stringify(meta),
        headers: { 'Content-Type': 'application/json' }
      }));
    } catch (e) {
      // Handle index update failures silently
    }
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
    if (request.method === 'POST' && request.url.endsWith('/delete')) {
      await this.state.storage.deleteAll();
      return new Response(JSON.stringify({ ok: true, message: 'Room deleted' }), { 
        headers: { 'Content-Type': 'application/json' } 
      });
    }

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
      if (path === '/leaveRoom' && request.method === 'POST') return this._handleLeave(request);
      if (path === '/rematch' && request.method === 'POST') return this._handleRematch(request);
      if (path === '/getState' && request.method === 'GET') return this._handleGetState();
      if (path === '/heartbeat' && request.method === 'POST') return this._handleHeartbeat(request);

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
    server.serializeAttachment({ playerId });

    try {
      this._sockets.add(server);
      server.addEventListener('close', () => {
        try { this._sockets.delete(server); } catch (e) {}
      });
    } catch (e) {}

    server.send(JSON.stringify({ type: 'init', room: this.room }));

    return new Response(null, { status: 101, webSocket: client });
  }

  _broadcastUpdate() {
    const msg = JSON.stringify({ type: 'update', room: this.room });
    for (const ws of this._sockets) {
      try {
        if (ws && ws.readyState === 1) ws.send(msg);
      } catch (e) {}
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
    this.room.private = body.private || false;
    this.room.createdAt = now;
    this.room.phase = 'LOBBY';
    
    if (body.creatorName && body.creatorPlayerId) {
      this.room.players.push({ 
        id: body.creatorPlayerId, 
        name: body.creatorName, 
        joinedAt: now 
      });
    }
    
    if (body.queuedPlayers && Array.isArray(body.queuedPlayers)) {
      for (const queuedPlayer of body.queuedPlayers) {
        if (!this.room.players.some(p => p.id === queuedPlayer.playerId)) {
          this.room.players.push({
            id: queuedPlayer.playerId,
            name: queuedPlayer.name,
            joinedAt: now
          });
        }
      }
    }
    
    await this._save();
    return this._response({ ok: true, roomId: this.room.roomId });
  }

  async _handleJoin(request) {
    console.log('🚪 _handleJoin called for room:', this.room?.roomId);
    if (this.room.updatedAt && (this._now() - this.room.updatedAt) > 5 * 60 * 1000) {
      try {
        if (this.env?.ROOM_INDEX) {
          const indexId = this.env.ROOM_INDEX.idFromName('index');
          const obj = this.env.ROOM_INDEX.get(indexId);
          await obj.fetch(new Request('https://do/remove', {
            method: 'POST',
            body: JSON.stringify({ roomId: this.room.roomId }),
            headers: { 'Content-Type': 'application/json' }
          }));
        }
      } catch (e) {}
      this.room.removedAt = this._now();
      await this._save();
      return this._response({ error: 'room_too_old' }, 410);
    }
    if (this.room.closed) {
      return this._response({ error: 'room_closed' }, 410);
    }
    const body = await request.json();
    const { playerId, name } = body;
    if (!playerId) return this._response({ error: 'playerId_required' }, 400);
    if (this.room.phase !== 'LOBBY') return this._response({ error: 'not_in_lobby' }, 400);
    if (this.room.players.find(p => p.id === playerId)) {
      return this._response({ ok: true, room: this.room });
    }
    if (this.room.players.length >= this.room.maxPlayers) return this._response({ error: 'room_full' }, 400);
    this.room.players.push({ id: playerId, name: name || null, joinedAt: this._now() });
    await this._save();
    return this._response({ ok: true, room: this.room });
  }

  async _handleStartBidding(request) {
    const body = await request.json().catch(() => ({}));
    const { playerId } = body;
    if (!playerId) return this._response({ error: 'playerId_required' }, 400);
    if (this.room.phase !== 'LOBBY') return this._response({ error: 'invalid_phase' }, 400);
    if (this.room.players.length !== this.room.maxPlayers) return this._response({ error: 'need_more_players' }, 400);
    const now = this._now();

    if (this.room.startRequestedBy && this.room.startConfirmDeadline) {
      if (this.room.startRequestedBy === playerId) return this._response({ ok: true, message: 'already_requested' });
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
        this.room.startRequestedBy = null;
        this.room.startConfirmDeadline = null;
        await this._save();
        return this._response({ ok: true, bidDeadline: this.room.bidDeadline });
      }
      this.room.startRequestedBy = null;
      this.room.startConfirmDeadline = null;
      await this._save();
      return this._response({ error: 'start_request_expired' }, 400);
    }

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
    const [p1, p2] = this.room.players.map(p => p.id);
    const bids = this.room.bids || {};

    const b1 = bids[p1];
    const b2 = bids[p2];
    const deadlinePassed = this.room.bidDeadline && now > this.room.bidDeadline;

    if (!b1 || !b2) {
      if (!deadlinePassed) return;
      if (!b1) bids[p1] = { amount: this.room.mainTimeMs, submittedAt: now };
      if (!b2) bids[p2] = { amount: this.room.mainTimeMs, submittedAt: now };
    }

    const updatedB1 = bids[p1];
    const updatedB2 = bids[p2];

    if (updatedB1.amount === updatedB2.amount) {
      this.room.bids = {};
      this.room.bidDeadline = now + this.room.bidDurationMs;
      await this._save();
      return;
    }

    if (updatedB1.amount < updatedB2.amount) {
      this.room.winnerId = p1;
      this.room.loserId = p2;
      this.room.winningBidMs = updatedB1.amount;
      this.room.losingBidMs = updatedB2.amount;
    } else {
      this.room.winnerId = p2;
      this.room.loserId = p1;
      this.room.winningBidMs = updatedB2.amount;
      this.room.losingBidMs = updatedB1.amount;
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
    await this._indexUpdate();
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

    if (this.room.disconnectedPlayerId === playerId) {
      this.room.disconnectedPlayerId = null;
      this.room.disconnectStart = null;
    }
    this.room.updatedAt = now;

    const game = new Chess(this.room.gameFen || undefined);

    const elapsed = now - (this.room.clocks.lastTickAt || now);
    if (playerColor === 'white') this.room.clocks.whiteRemainingMs -= elapsed;
    else this.room.clocks.blackRemainingMs -= elapsed;

    const whiteFlagged = this.room.clocks.whiteRemainingMs <= 0;
    const blackFlagged = this.room.clocks.blackRemainingMs <= 0;

    if ((playerColor === 'white' && whiteFlagged) || (playerColor === 'black' && blackFlagged)) {
      const opponentColorLetter = playerColor === 'white' ? 'b' : 'w';

      const pieces = game.board().flat().filter(Boolean);
      const opponentPieces = pieces.filter(p => p.color === opponentColorLetter);
      const opponentNonKing = opponentPieces.filter(p => p.type !== 'k');

      const hasMajorOrPawn = opponentNonKing.some(p =>
        p.type === 'q' || p.type === 'r' || p.type === 'p'
      );
      const hasMultipleMinors = opponentNonKing.length > 1;

      const canEverMate = hasMajorOrPawn || hasMultipleMinors;

      if (!canEverMate) {
        this.room.phase = 'FINISHED';
        this.room.winnerId = null;
        this.room.clocks.frozenAt = now;
        this.room.rematchWindowEnds = this._now() + 10 * 1000;
        this.room.rematchVotes = {};
        await this._save();
        return this._response({
          ok: true,
          result: 'draw',
          reason: 'timeout_but_opponent_cannot_mate',
          clocks: this.room.clocks,
          moves: this.room.moves,
          rematchWindowEnds: this.room.rematchWindowEnds
        });
      }

      const winnerId = this.room.players.find(p => this.room.colors[p.id] !== playerColor)?.id || null;
      this.room.phase = 'FINISHED';
      this.room.winnerId = winnerId;
      this.room.clocks.frozenAt = now; 
      this.room.rematchWindowEnds = this._now() + 15 * 1000;
      this.room.rematchVotes = {};
      await this._save();
      return this._response({
        ok: true,
        result: 'time_forfeit',
        winnerId,
        clocks: this.room.clocks,
        moves: this.room.moves,
        rematchWindowEnds: this.room.rematchWindowEnds
      });
    }

    if (typeof move !== 'string' || move.length < 4) return this._response({ error: 'invalid_move_format' }, 400);
    const from = move.slice(0, 2);
    const to = move.slice(2, 4);
    const promotion = move.length >= 5 ? move[4] : undefined;

    const moved = game.move({ from, to, promotion });
    if (!moved) return this._response({ error: 'illegal_move' }, 400);

    this.room.gameFen = game.fen();
    this.room.moves.push({ by: playerId, move, at: now });
    this.room.clocks.lastTickAt = now;
    this.room.clocks.turn = this.room.clocks.turn === 'white' ? 'black' : 'white';

    if (game.isCheckmate()) {
      this.room.phase = 'FINISHED';
      this.room.winnerId = playerId;
      this.room.clocks.frozenAt = now;  
      this.room.rematchWindowEnds = this._now() + 15 * 1000;
      this.room.rematchVotes = {};
      await this._save();
      return this._response({
        ok: true,
        result: 'checkmate',
        winnerId: playerId,
        clocks: this.room.clocks,
        moves: this.room.moves,
        rematchWindowEnds: this.room.rematchWindowEnds
      });
    }

    let drawReason = null;
    if (game.isStalemate()) drawReason = 'stalemate';
    else if (game.isInsufficientMaterial()) drawReason = 'insufficient_material';
    else if (game.isThreefoldRepetition()) drawReason = 'threefold_repetition';
    else if (game.isDraw()) drawReason = 'draw'; // This includes fifty-move rule

    if (drawReason) {
      this.room.phase = 'FINISHED';
      this.room.winnerId = null;
      this.room.clocks.frozenAt = now;
      this.room.rematchWindowEnds = this._now() + 15 * 1000;
      this.room.rematchVotes = {};
      await this._save();
      return this._response({
        ok: true,
        result: 'draw',
        reason: drawReason,
        clocks: this.room.clocks,
        moves: this.room.moves,
        rematchWindowEnds: this.room.rematchWindowEnds
      });
    }

    await this._save();
    await this._indexUpdate();
    return this._response({ ok: true, clocks: this.room.clocks, moves: this.room.moves });
  }

  async _handleTimeForfeit(request) {
    const body = await request.json().catch(() => ({}));
    const { timedOutPlayerId } = body;

    if (this.room.phase !== 'PLAYING') return this._response({ error: 'invalid_phase' }, 400);
    if (!timedOutPlayerId) return this._response({ error: 'timedOutPlayerId_required' }, 400);

    const game = new Chess(this.room.gameFen || undefined);

    const opponent = this.room.players.find(p => p.id !== timedOutPlayerId) || null;
    const opponentId = opponent ? opponent.id : null;
    const opponentColor = opponentId && this.room.colors ? this.room.colors[opponentId] : null;
    const opponentColorLetter = opponentColor === 'white' ? 'w' : (opponentColor === 'black' ? 'b' : null);

    let result = 'time_forfeit';
    let reason = null;
    let winnerId = opponentId;

    if (opponentColorLetter) {
      const pieces = game.board().flat().filter(Boolean);
      const opponentPieces = pieces.filter(p => p.color === opponentColorLetter);
      const opponentNonKing = opponentPieces.filter(p => p.type !== 'k');

      const hasMajorOrPawn = opponentNonKing.some(p =>
        p.type === 'q' || p.type === 'r' || p.type === 'p'
      );
      const hasMultipleMinors = opponentNonKing.length > 1;

      const canEverMate = hasMajorOrPawn || hasMultipleMinors;

      if (!canEverMate) {
        result = 'draw';
        reason = 'timeout_but_opponent_cannot_mate';
        winnerId = null;
      }
    }

    this.room.phase = 'FINISHED';
    this.room.winnerId = winnerId;
    this.room.rematchWindowEnds = this._now() + 60 * 1000;
    this.room.rematchVotes = {};
    await this._save();

    const responseBody = {
      ok: true,
      result,
      winnerId,
      rematchWindowEnds: this.room.rematchWindowEnds
    };
    if (reason) responseBody.reason = reason;

    return this._response(responseBody);
  }

  async _handleGetState() {
    const now = this._now();
    let saveNeeded = false;

    await this._resolveBidsIfNeeded();
    await this._resolveChoiceIfNeeded();

    if (this.room.updatedAt && (now - this.room.updatedAt) > 5 * 60 * 1000) {
      try {
        if (this.env?.ROOM_INDEX) {
          const indexId = this.env.ROOM_INDEX.idFromName('index');
          const obj = this.env.ROOM_INDEX.get(indexId);
          await obj.fetch(new Request('https://do/remove', {
            method: 'POST',
            body: JSON.stringify({ roomId: this.room.roomId }),
            headers: { 'Content-Type': 'application/json' }
          }));
        }
      } catch (e) {}
      await this.state.storage.delete('room');
      return this._response({ error: 'room_expired' }, 410);
    }

    if (this.room.phase === 'PLAYING') {
      if (!this.room.disconnectedPlayerId && (now - (this.room.updatedAt || 0)) > 10000) {
        const activePlayerId = Object.keys(this.room.colors || {}).find(id => 
          this.room.colors[id] === this.room.clocks.turn
        );
        this.room.disconnectedPlayerId = activePlayerId ? 
          Object.keys(this.room.colors || {}).find(id => id !== activePlayerId) : null;
        if (this.room.disconnectedPlayerId) {
          this.room.disconnectStart = now;
          saveNeeded = true;
        }
      }

      if (this.room.disconnectedPlayerId && this.room.disconnectStart && 
          (now - this.room.disconnectStart) > this.room.disconnectTimeoutMs) {
        const winnerId = this.room.disconnectedPlayerId === Object.keys(this.room.colors || {})[0] ? 
          Object.keys(this.room.colors || {})[1] : Object.keys(this.room.colors || {})[0];
        this.room.phase = 'FINISHED';
        this.room.winnerId = winnerId;
        this.room.closeReason = 'disconnect_forfeit';
        saveNeeded = true;
      }
    }

    if (!this.room.closed && this.room.startConfirmDeadline && now > this.room.startConfirmDeadline) {
      this.room.startRequestedBy = null;
      this.room.startConfirmDeadline = null;
      this.room.closed = true;
      this.room.closeReason = 'start_expired';
      this.room.closedAt = now;
      saveNeeded = true;
    }

    if (this.room.closed && this.room.closeReason === 'start_expired' && this.room.closedAt && (now - this.room.closedAt) > 10 * 60 * 1000) {
      try {
        if (this.env?.ROOM_INDEX) {
          const indexId = this.env.ROOM_INDEX.idFromName('index');
          const obj = this.env.ROOM_INDEX.get(indexId);
          await obj.fetch(new Request('https://do/remove', {
            method: 'POST',
            body: JSON.stringify({ roomId: this.room.roomId }),
            headers: { 'Content-Type': 'application/json' }
          }));
        }
      } catch (e) {}
      this.room.removedAt = now;
      saveNeeded = true;
    }

    if (this.room.phase === 'FINISHED' && this.room.rematchWindowEnds && now > this.room.rematchWindowEnds) {
      const players = (this.room.players || []).map(p => p.id);
      const votes = this.room.rematchVotes || {};
      const bothAgreed = players.length > 0 && players.every(pid => votes[pid] === true);
      const anyNo = players.some(pid => votes[pid] === false);
      const yesVotes = players.filter(pid => votes[pid] === true);
      
      if (!bothAgreed) {
        try {
          if (this.env?.ROOM_INDEX) {
            const indexId = this.env.ROOM_INDEX.idFromName('index');
            const obj = this.env.ROOM_INDEX.get(indexId);
            await obj.fetch(new Request('https://do/remove', {
              method: 'POST',
              body: JSON.stringify({ roomId: this.room.roomId }),
              headers: { 'Content-Type': 'application/json' }
            }));
          }
        } catch (e) {}
        
        // Auto-requeue yes voters into new game with same time control
        if (yesVotes.length > 0) {
          for (const yesVoterId of yesVotes) {
            try {
              if (this.env?.ROOM_INDEX) {
                const indexId = this.env.ROOM_INDEX.idFromName('index');
                const obj = this.env.ROOM_INDEX.get(indexId);
                await obj.fetch(new Request('https://do/update', {
                  method: 'POST',
                  body: JSON.stringify({
                    roomId: this.room.roomId,
                    phase: 'LOBBY',
                    maxPlayers: 2,
                    private: false,
                    mainTimeMs: this.room.mainTimeMs,
                    players: this.room.players,
                    updatedAt: now
                  }),
                  headers: { 'Content-Type': 'application/json' }
                }));
              }
            } catch (e) {}
          }
        }
        
        this.room.closed = true;
        this.room.closeReason = anyNo ? 'declined_rematch' : 'rematch_timeout';
        this.room.closedAt = now;
        await this._save();
      }
    }

    if (saveNeeded) await this._save();

    return this._response({ ok: true, room: this.room });
  }
  
  async _handleRematch(request) {
    const body = await request.json().catch(() => ({}));
    const { playerId, agree } = body;
    if (!playerId) return this._response({ error: 'playerId_required' }, 400);
    if (this.room.phase !== 'FINISHED') return this._response({ error: 'not_finished' }, 400);
    if (!this.room.rematchWindowEnds || this._now() > this.room.rematchWindowEnds) return this._response({ error: 'rematch_window_closed' }, 400);

    this.room.rematchVotes = this.room.rematchVotes || {};
    
    // Lock in vote - no changes allowed
    if (typeof this.room.rematchVotes[playerId] !== 'undefined') {
      return this._response({ error: 'already_voted' }, 400);
    }
    
    this.room.rematchVotes[playerId] = !!agree;
    await this._save();

    const players = (this.room.players || []).map(p => p.id);
    const allAgreed = players.length > 0 && players.every(pid => this.room.rematchVotes[pid] === true);
    
    if (allAgreed) {
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

    const anyNo = players.some(pid => this.room.rematchVotes[pid] === false);
    if (anyNo) {
      // Immediate closure on "No" vote - no waiting period
      try {
        if (this.env?.ROOM_INDEX) {
          const indexId = this.env.ROOM_INDEX.idFromName('index');
          const obj = this.env.ROOM_INDEX.get(indexId);
          await obj.fetch(new Request('https://do/remove', {
            method: 'POST',
            body: JSON.stringify({ roomId: this.room.roomId }),
            headers: { 'Content-Type': 'application/json' }
          }));
        }
      } catch (e) {}
      
      // Auto-requeue yes voters into queue system
      const yesVotes = players.filter(pid => this.room.rematchVotes[pid] === true);
      if (yesVotes.length > 0) {
        for (const yesVoterId of yesVotes) {
          try {
            // Get the player who voted yes
            const yesPlayer = this.room.players.find(p => p.id === yesVoterId);
            if (yesPlayer) {
              // Add to queue with same time control
              if (this.env?.ROOM_INDEX) {
                const indexId = this.env.ROOM_INDEX.idFromName('index');
                const obj = this.env.ROOM_INDEX.get(indexId);
                await obj.fetch(new Request('https://do/addToQueue', {
                  method: 'POST',
                  body: JSON.stringify({
                    playerId: yesVoterId,
                    name: yesPlayer.name,
                    mainTimeMs: this.room.mainTimeMs
                  }),
                  headers: { 'Content-Type': 'application/json' }
                }));
              }
            }
          } catch (e) {
            console.error('Failed to requeue yes voter:', e);
          }
        }
      }
      
      this.room.closed = true;
      this.room.closeReason = 'declined_rematch';
      this.room.closedAt = this._now();
      await this._save();
      return this._response({ ok: true, rematchStarted: false, voteResult: 'no_vote', votes: this.room.rematchVotes });
    }

    const yesVotes = players.filter(pid => this.room.rematchVotes[pid] === true);
    
    if (yesVotes.length === 1) {
      return this._response({ ok: true, rematchStarted: false, voteResult: 'waiting_for_opponent', votes: this.room.rematchVotes });
    }

    return this._response({ ok: true, rematchStarted: false, votes: this.room.rematchVotes });
  }

    async _handleLeave(request) {
      const body = await request.json().catch(() => ({}));
      const { playerId } = body;
      if (!playerId) return this._response({ error: 'playerId_required' }, 400);

      const beforeCount = (this.room.players || []).length;
      this.room.players = (this.room.players || []).filter(p => p.id !== playerId);

      if (this.room.players.length === beforeCount) {
        return this._response({ ok: true, room: this.room });
      }

      await this._save();
      return this._response({ ok: true, room: this.room });
    }

    async _handleHeartbeat(request) {
      const { playerId } = await request.json().catch(() => ({}));
      if (playerId) {
        this.room.updatedAt = this._now();
      }
      return this._response({ ok: true });
    }




}

export class RoomIndex {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this._sockets = new Set();
  }

  async _getAll() {
    const raw = await this.state.storage.get('rooms');
    return raw || {};
  }

  async _saveAll(obj) {
    await this.state.storage.put('rooms', obj);
  }

  _handleWebSocket(request) {
    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);

    server.accept();
    this._sockets.add(server);

    server.addEventListener('close', () => {
      try { this._sockets.delete(server); } catch (e) {}
    });

    // Send initial queue status
    this._broadcastQueueStatus();

    return new Response(null, { status: 101, webSocket: client });
  }

  _broadcastQueueStatus() {
    const msg = JSON.stringify({ 
      type: 'queue_update',
      timestamp: Date.now()
    });
    for (const ws of this._sockets) {
      try {
        if (ws && ws.readyState === 1) ws.send(msg);
      } catch (e) {}
    }
  }

  async _cleanupStaleQueues(queues) {
    const now = Date.now();
    const STALE_TIME = 5 * 60 * 1000;
    
    for (const timeKey in queues) {
      const originalLength = queues[timeKey].length;
      queues[timeKey] = queues[timeKey].filter(player => {
        const lastActivity = player.lastHeartbeat || player.joinedAt;
        return (now - lastActivity) < STALE_TIME;
      });
      
      if (queues[timeKey].length < originalLength) {
        console.log(`Cleaned ${originalLength - queues[timeKey].length} stale players from ${timeKey}ms queue`);
      }
    }
    
    await this.state.storage.put('queues', queues);
  }

  async _getEstimatedWaitTimes() {
    const rooms = await this._getAll();
    const queues = await this.state.storage.get('queues') || {};
    const estimates = {};
    const now = Date.now();
    
    for (const timeControl of TIME_CONTROLS) {
      const timeMs = timeControl.ms;
      const timeKey = timeMs.toString();
      const queueLength = queues[timeKey]?.length || 0;
      
      const activeGames = Object.values(rooms).filter(room => 
        room.mainTimeMs === timeMs && 
        room.phase === 'PLAYING' &&
        room.players?.length === 2
      );
      
      let estimateData = {
        type: 'none',
        message: 'No games in place - no available estimate'
      };
      
      if (queueLength >= 1) {
        estimateData = {
          type: 'match_now',
          message: 'Match NOW!'
        };
      } else if (queueLength === 0 && activeGames.length > 0) {
        // Find game with shortest time remaining
        let minRemainingTime = Infinity;
        let shortestGame = null;
        
        for (const game of activeGames) {
          // Try multiple clock field locations, fallback to time-based estimation
          const whiteTime = game.liveWhiteMs || game.clocks?.whiteRemainingMs || game.whiteTime || 0;
          const blackTime = game.liveBlackMs || game.clocks?.blackRemainingMs || game.blackTime || 0;
          
          let minPlayerTime;
          if (whiteTime > 0 || blackTime > 0) {
            // Use actual clock data if available
            minPlayerTime = Math.min(whiteTime, blackTime);
          } else {
            // Without real clock data, we can't accurately estimate chess clock time
            // Chess clocks are move-based, not elapsed time based
            // Just show that a game is active
            minPlayerTime = (game.mainTimeMs || 300000); // Use full time as placeholder
          }
          
          if (minPlayerTime < minRemainingTime) {
            minRemainingTime = minPlayerTime;
            shortestGame = game;
          }
        }
        
        if (minRemainingTime < Infinity && shortestGame) {
          // Check if we have real clock data or just placeholder
          const hasRealClockData = (shortestGame.liveWhiteMs || shortestGame.clocks?.whiteRemainingMs || 
                                   shortestGame.liveBlackMs || shortestGame.clocks?.blackRemainingMs || 0) > 0;
          
          if (hasRealClockData) {
            // Use real clock data for countdown
            const anchorKey = `estimate_anchor_${timeKey}`;
            const anchorData = await this.state.storage.get(anchorKey);
            
            if (!anchorData || anchorData.gameId !== shortestGame.roomId) {
              // Create new anchor
              const newAnchor = {
                gameId: shortestGame.roomId,
                startTime: now,
                durationMs: minRemainingTime,
                timeControlMs: timeMs
              };
              await this.state.storage.put(anchorKey, newAnchor);
              
              estimateData = {
                type: 'countdown',
                startTime: now,
                durationMs: minRemainingTime,
                message: `Next game in ~${Math.ceil(minRemainingTime / 60000)} min`
              };
            } else {
              // Use existing anchor - countdown continues
              const elapsed = now - anchorData.startTime;
              const remaining = Math.max(0, anchorData.durationMs - elapsed);
              
              estimateData = {
                type: 'countdown',
                startTime: anchorData.startTime,
                durationMs: anchorData.durationMs,
                message: `Next game in ~${Math.ceil(remaining / 60000)} min`
              };
            }
          } else {
            estimateData = {
              type: 'games_active',
              message: `${activeGames.length} game${activeGames.length > 1 ? 's' : ''} in progress`
            };
          }
        }
      }
      
      estimates[timeKey] = {
        queueLength,
        activeGames: activeGames.length,
        estimate: estimateData
      };
    }
    
    return estimates;
  }

  async fetch(request) {
    if (request.method === 'POST' && request.url.endsWith('/update')) {
      const body = await request.json().catch(() => ({}));
      const rooms = await this._getAll();
      const existingRoom = rooms[body.roomId] || {};
      
      rooms[body.roomId] = { 
        ...existingRoom,
        roomId: body.roomId, 
        phase: body.phase, 
        players: body.players !== undefined ? body.players : (existingRoom.players || []),
        updatedAt: body.updatedAt || Date.now(),
        clocks: body.clocks,
        mainTimeMs: body.mainTimeMs,
        liveWhiteMs: body.liveWhiteMs,
        liveBlackMs: body.liveBlackMs
      };
      
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
    if (request.method === 'POST' && request.url.endsWith('/clear')) {
      await this._saveAll({});
      return new Response(JSON.stringify({ ok: true, message: 'All rooms cleared from index' }), { headers: { 'Content-Type': 'application/json' } });
    }
    if (request.method === 'GET' && request.url.endsWith('/list')) {
      const rooms = await this._getAll();
      const list = Object.values(rooms).filter(r => r.phase !== 'FINISHED');
      return new Response(JSON.stringify({ ok: true, rooms: list }), { headers: { 'Content-Type': 'application/json' } });
    }
    
    if (request.method === 'POST' && request.url.endsWith('/joinAll')) {
      const body = await request.json().catch(() => ({}));
      const { playerId, name } = body;
      
      const queues = await this.state.storage.get('queues') || {};
      
      // Add player to all time control queues (don't remove from existing)
      for (const timeControl of TIME_CONTROLS) {
        const timeKey = timeControl.ms.toString();
        if (!queues[timeKey]) {
          queues[timeKey] = [];
        }
        
        // Check if player already in this queue
        const alreadyInQueue = queues[timeKey].some(p => p.playerId === playerId);
        if (!alreadyInQueue) {
          queues[timeKey].push({ 
            playerId, 
            name, 
            joinedAt: Date.now(), 
            lastHeartbeat: Date.now() 
          });
        } else {
          // Update heartbeat if already in queue
          const player = queues[timeKey].find(p => p.playerId === playerId);
          if (player) {
            player.lastHeartbeat = Date.now();
          }
        }
      }
      
      await this.state.storage.put('queues', queues);
      
      // Clean up stale players before checking for matches
      await this._cleanupStaleQueues(queues);
      
      // Check if any queue has 2 players to create a room
      console.log('🔍 Checking all queues for matches after joinAll...');
      for (const timeControl of TIME_CONTROLS) {
        const timeKey = timeControl.ms.toString();
        console.log(`📊 Queue ${timeKey}: ${queues[timeKey].length} players`);
        if (queues[timeKey].length >= 2) {
          const queuedPlayers = queues[timeKey].slice(0, 2);
          console.log('🏠 Found match in joinAll:', queuedPlayers);
          return new Response(JSON.stringify({ 
            shouldCreateRoom: true, 
            mainTimeMs: timeControl.ms, 
            queuedPlayers 
          }), { headers: { 'Content-Type': 'application/json' } });
        }
      }
      
      return new Response(JSON.stringify({ 
        ok: true, 
        queued: true,
        joinedQueues: TIME_CONTROLS.map(tc => tc.minutes)
      }), { headers: { 'Content-Type': 'application/json' } });
    }
    
    if (request.method === 'POST' && request.url.endsWith('/addToQueue')) {
      const body = await request.json().catch(() => ({}));
      const { playerId, name, mainTimeMs } = body;
      
      console.log('🎯 addToQueue called:', { playerId, name, mainTimeMs });
      
      const queues = await this.state.storage.get('queues') || {};
      const timeKey = mainTimeMs.toString();
      
      console.log('📊 Current queues before:', JSON.stringify(queues));
      
      if (!queues[timeKey]) {
        queues[timeKey] = [];
      }
      
      // Check if player already in this queue
      const alreadyInQueue = queues[timeKey].some(p => p.playerId === playerId);
      if (!alreadyInQueue) {
        queues[timeKey].push({ playerId, name, joinedAt: Date.now(), lastHeartbeat: Date.now() });
        console.log('➕ Player added to queue:', timeKey);
      } else {
        // Update heartbeat if already in queue
        const player = queues[timeKey].find(p => p.playerId === playerId);
        if (player) {
          player.lastHeartbeat = Date.now();
          console.log('💓 Player heartbeat updated:', timeKey);
        }
      }
      
      await this.state.storage.put('queues', queues);
      console.log('💾 Queues saved:', JSON.stringify(queues));
      
      // Broadcast queue update to all connected clients
      this._broadcastQueueStatus();
      
      // Clean up stale players before checking for matches
      await this._cleanupStaleQueues(queues);
      console.log('🧹 After cleanup:', JSON.stringify(queues));
      
      // Check if we have 2 players to create a room
      if (queues[timeKey].length >= 2) {
        const queuedPlayers = queues[timeKey].slice(0, 2);
        console.log('🏠 Creating room with players:', queuedPlayers);
        return new Response(JSON.stringify({ 
          shouldCreateRoom: true, 
          mainTimeMs, 
          queuedPlayers 
        }), { headers: { 'Content-Type': 'application/json' } });
      }
      
      console.log('⏳ Player queued, position:', queues[timeKey].findIndex(p => p.playerId === playerId) + 1);
      return new Response(JSON.stringify({ 
        ok: true, 
        queued: true, 
        queuePosition: queues[timeKey].findIndex(p => p.playerId === playerId) + 1 
      }), { headers: { 'Content-Type': 'application/json' } });
    }
    
    if (request.method === 'POST' && request.url.endsWith('/debugQueues')) {
      const queues = await this.state.storage.get('queues') || {};
      console.log('🔍 Debug: All queues:', JSON.stringify(queues, null, 2));
      return new Response(JSON.stringify({ 
        queues,
        totalQueued: Object.values(queues).reduce((sum, queue) => sum + queue.length, 0)
      }), { headers: { 'Content-Type': 'application/json' } });
    }
    
    if (request.method === 'POST' && request.url.endsWith('/debugRooms')) {
      const rooms = await this._getAll();
      console.log('🔍 Debug: All rooms:', rooms);
      return new Response(JSON.stringify({ 
        rooms,
        count: Object.keys(rooms).length
      }), { headers: { 'Content-Type': 'application/json' } });
    }
    
    if (request.method === 'POST' && request.url.endsWith('/checkMatch')) {
      const body = await request.json().catch(() => ({}));
      const { playerId } = body;
      
      const queues = await this.state.storage.get('queues') || {};
      const rooms = await this._getAll();
      
      console.log('🔍 checkMatch called for:', playerId);
      
      // Check if player is in any active room
      console.log('🏠 Checking rooms for player:', playerId);
      console.log('🏠 Available rooms:', Object.keys(rooms));
      
      for (const room of Object.values(rooms)) {
        console.log('🏠 Checking room:', room.roomId, 'players:', room.players);
        if (room.players && Array.isArray(room.players)) {
          // Check both possible player ID field names
          const foundInRoom = room.players.some(p => {
            console.log('🔍 Checking player:', p, 'against:', playerId);
            return p.id === playerId || p.playerId === playerId;
          });
          if (foundInRoom) {
            console.log('✅ Player found in room:', room.roomId);
            return new Response(JSON.stringify({ 
              matched: true, 
              roomId: room.roomId,
              room 
            }), { headers: { 'Content-Type': 'application/json' } });
          }
        }
      }
      
      // Check if player is still in any queue
      let inQueue = false;
      for (const timeKey in queues) {
        if (queues[timeKey].some(p => p.playerId === playerId)) {
          inQueue = true;
          break;
        }
      }
      
      console.log('📊 Match check result:', { matched: false, inQueue });
      return new Response(JSON.stringify({ 
        matched: false, 
        inQueue 
      }), { headers: { 'Content-Type': 'application/json' } });
    }
    
    if (request.method === 'POST' && request.url.endsWith('/heartbeat')) {
      const body = await request.json().catch(() => ({}));
      const { playerId } = body;
      
      const queues = await this.state.storage.get('queues') || {};
      let found = false;
      
      for (const timeKey in queues) {
        const player = queues[timeKey].find(p => p.playerId === playerId);
        if (player) {
          player.lastHeartbeat = Date.now();
          found = true;
          break;
        }
      }
      
      if (found) {
        await this.state.storage.put('queues', queues);
      }
      
      return new Response(JSON.stringify({ ok: true }), { headers: { 'Content-Type': 'application/json' } });
    }
    
    if (request.method === 'POST' && request.url.endsWith('/removeFromAllQueues')) {
      const body = await request.json().catch(() => ({}));
      const { playerIds } = body;
      
      if (!playerIds || !Array.isArray(playerIds)) {
        return new Response(JSON.stringify({ error: 'playerIds_required' }), { 
          status: 400, 
          headers: { 'Content-Type': 'application/json' } 
        });
      }
      
      const queues = await this.state.storage.get('queues') || {};
      
      for (const timeKey in queues) {
        queues[timeKey] = queues[timeKey].filter(p => !playerIds.includes(p.playerId));
      }
      
      await this.state.storage.put('queues', queues);
      
      // Broadcast queue update to all connected clients
      this._broadcastQueueStatus();
      
      return new Response(JSON.stringify({ ok: true }), { 
        headers: { 'Content-Type': 'application/json' } 
      });
    }
    
    if (request.method === 'POST' && request.url.endsWith('/updateClocks')) {
      const body = await request.json().catch(() => ({}));
      const { roomId, clocks } = body;
      
      if (!roomId || !clocks) {
        return new Response(JSON.stringify({ error: 'roomId_and_clocks_required' }), { 
          status: 400, 
          headers: { 'Content-Type': 'application/json' } 
        });
      }
      
      const rooms = await this._getAll();
      const existingRoom = rooms[roomId] || {};
      
      // Update the room with clock data
      rooms[roomId] = { 
        ...existingRoom,
        roomId,
        clocks,
        updatedAt: Date.now()
      };
      
      await this._putAll(rooms);
      
      return new Response(JSON.stringify({ ok: true }), { 
        headers: { 'Content-Type': 'application/json' } 
      });
    }
    
    if (request.method === 'POST' && request.url.endsWith('/debugEstimates')) {
      const rooms = await this._getAll();
      const queues = await this.state.storage.get('queues') || {};
      const now = Date.now();
      
      const debugInfo = {
        timestamp: new Date(now).toISOString(),
        queues: {},
        activeGames: {},
        anchors: {},
        estimates: {}
      };
      
      // Debug queue info
      for (const timeControl of TIME_CONTROLS) {
        const timeKey = timeControl.ms.toString();
        debugInfo.queues[timeKey] = {
          timeControl: timeControl.display,
          queueLength: queues[timeKey]?.length || 0,
          players: queues[timeKey] || []
        };
      }
      
      debugInfo.allRooms = {};
      for (const [roomId, room] of Object.entries(rooms)) {
        debugInfo.allRooms[roomId] = {
          phase: room.phase,
          players: room.players?.length || 0,
          playerNames: room.players?.map(p => ({id: p.id, name: p.name})) || [],
          mainTimeMs: room.mainTimeMs,
          allFields: Object.keys(room).filter(key => 
            key.toLowerCase().includes('time') || 
            key.toLowerCase().includes('clock') ||
            key.toLowerCase().includes('white') ||
            key.toLowerCase().includes('black') ||
            key.toLowerCase().includes('remaining')
          ),
          timeFields: {
            liveWhiteMs: room.liveWhiteMs,
            liveBlackMs: room.liveBlackMs,
            clocks: room.clocks,
            whiteTime: room.whiteTime,
            blackTime: room.blackTime,
            whiteRemainingMs: room.clocks?.whiteRemainingMs,
            blackRemainingMs: room.clocks?.blackRemainingMs,
            time: room.time,
            turn: room.turn,
            createdAt: room.createdAt,
            updatedAt: room.updatedAt
          },
          wouldBeDetected: room.phase === 'PLAYING' && room.players?.length === 2,
          updatedAt: new Date(room.updatedAt).toISOString(),
          roomId: roomId
        };
      }
      for (const [roomId, room] of Object.entries(rooms)) {
        if (room.phase === 'PLAYING' && room.players?.length === 2) {
          const timeKey = room.mainTimeMs?.toString();
          if (timeKey) {
            if (!debugInfo.activeGames[timeKey]) {
              debugInfo.activeGames[timeKey] = [];
            }
            
            // Try multiple clock field locations, fallback to time-based estimation
            const whiteTime = room.liveWhiteMs || room.clocks?.whiteRemainingMs || room.whiteTime || 0;
            const blackTime = room.liveBlackMs || room.clocks?.blackRemainingMs || room.blackTime || 0;
            
            let minPlayerTime;
            let timeSource;
            if (whiteTime > 0 || blackTime > 0) {
              // Use actual clock data if available
              minPlayerTime = Math.min(whiteTime, blackTime);
              timeSource = 'clock_data';
            } else {
              // Without real clock data, we can't accurately estimate chess clock time
              // Chess clocks are move-based, not elapsed time based
              // Just show that a game is active
              minPlayerTime = (room.mainTimeMs || 300000); // Use full time as placeholder
              timeSource = 'game_active_placeholder';
            }
            
            debugInfo.activeGames[timeKey].push({
              roomId,
              phase: room.phase,
              players: room.players.map(p => ({ id: p.id, name: p.name })),
              whiteTime: Math.round(whiteTime / 1000) + 's',
              blackTime: Math.round(blackTime / 1000) + 's',
              minPlayerTime: Math.round(minPlayerTime / 1000) + 's',
              updatedAt: new Date(room.updatedAt).toISOString(),
              // Show what clock fields we found and time source
              timeSource,
              clockFields: {
                liveWhiteMs: room.liveWhiteMs,
                liveBlackMs: room.liveBlackMs,
                clocks: room.clocks,
                whiteTime: room.whiteTime,
                blackTime: room.blackTime,
                mainTimeMs: room.mainTimeMs,
                gameStartTime: room.updatedAt || room.createdAt,
                elapsed: Math.round((now - (room.updatedAt || room.createdAt || Date.now())) / 1000) + 's'
              }
            });
          }
        }
      }
      
      // Debug anchors
      for (const timeControl of TIME_CONTROLS) {
        const timeKey = timeControl.ms.toString();
        const anchorKey = `estimate_anchor_${timeKey}`;
        const anchorData = await this.state.storage.get(anchorKey);
        
        if (anchorData) {
          debugInfo.anchors[timeKey] = {
            gameId: anchorData.gameId,
            startTime: new Date(anchorData.startTime).toISOString(),
            durationMs: Math.round(anchorData.durationMs / 1000) + 's',
            elapsedMs: Math.round((now - anchorData.startTime) / 1000) + 's',
            remainingMs: Math.round(Math.max(0, anchorData.durationMs - (now - anchorData.startTime)) / 1000) + 's'
          };
        }
      }
      
      // Debug final estimates
      const estimates = await this._getEstimatedWaitTimes();
      debugInfo.estimates = estimates;
      
      return new Response(JSON.stringify(debugInfo, null, 2), { 
        headers: { 'Content-Type': 'application/json' } 
      });
    }
    
    if (request.method === 'POST' && request.url.endsWith('/cleanup')) {
      const queues = await this.state.storage.get('queues') || {};
      await this._cleanupStaleQueues(queues);
      return new Response(JSON.stringify({ ok: true }), { headers: { 'Content-Type': 'application/json' } });
    }
    
    if (request.method === 'GET' && request.url.endsWith('/queue-status')) {
      const estimates = await this._getEstimatedWaitTimes();
      console.log('📊 Queue status requested, estimates:', estimates);
      
      // Also log raw queue data for debugging
      const queues = await this.state.storage.get('queues') || {};
      console.log('📋 Raw queue data:', JSON.stringify(queues));
      
      return new Response(JSON.stringify({ ok: true, estimates }), { headers: { 'Content-Type': 'application/json' } });
    }
    
    if (request.method === 'POST' && request.url.endsWith('/clear-all-queues')) {
      const body = await request.json().catch(() => ({}));
      const { mainTimeMs } = body;
      
      const queues = await this.state.storage.get('queues') || {};
      
      if (mainTimeMs) {
        // Clear specific queue
        const timeKey = mainTimeMs.toString();
        if (queues[timeKey]) {
          queues[timeKey] = [];
        }
      } else {
        // Clear all queues
        for (const timeKey in queues) {
          queues[timeKey] = [];
        }
      }
      
      await this.state.storage.put('queues', queues);
      return new Response(JSON.stringify({ ok: true, message: mainTimeMs ? `Queue ${mainTimeMs}ms cleared` : 'All queues cleared' }), {
        headers: { 'Content-Type': 'application/json' }
      });
    }

    if (request.method === 'POST' && request.url.endsWith('/clearQueue')) {
      const body = await request.json().catch(() => ({}));
      const { mainTimeMs } = body;
      
      const queues = await this.state.storage.get('queues') || {};
      const timeKey = mainTimeMs.toString();
      
      if (queues[timeKey]) {
        queues[timeKey] = [];
      }
      
      await this.state.storage.put('queues', queues);
      
      // Broadcast queue update to all connected clients
      this._broadcastQueueStatus();
      
      return new Response(JSON.stringify({ ok: true }), { headers: { 'Content-Type': 'application/json' } });
    }
    
    if (request.method === 'POST' && request.url.endsWith('/removeFromAllQueues')) {
      const body = await request.json().catch(() => ({}));
      const { playerId } = body;
      
      const queues = await this.state.storage.get('queues') || {};
      
      for (const timeKey in queues) {
        queues[timeKey] = queues[timeKey].filter(p => p.playerId !== playerId);
      }
      
      await this.state.storage.put('queues', queues);
      
      // Broadcast queue update to all connected clients
      this._broadcastQueueStatus();
      
      return new Response(JSON.stringify({ ok: true }), { headers: { 'Content-Type': 'application/json' } });
    }
    return new Response(JSON.stringify({ error: 'not_found' }), { status: 404, headers: { 'Content-Type': 'application/json' } });
  }
}

// Scalable time controls configuration (matches frontend)
const TIME_CONTROLS = [
  { minutes: 5, ms: 300000, display: '5 min' },
  { minutes: 10, ms: 600000, display: '10 min' },
  { minutes: 15, ms: 900000, display: '15 min' },
  // Easy to add more time controls:
  // { minutes: 3, ms: 180000, display: '3 min' },
  // { minutes: 30, ms: 1800000, display: '30 min' },
];

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const segments = url.pathname.replace(/(^\/|\/$)/g, '').split('/');
    const corsHeaders = {
      'Access-Control-Allow-Origin': env && env.FRONTEND_URL ? env.FRONTEND_URL : '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };
    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: corsHeaders });

    try {
      if (request.method === 'POST' && url.pathname === '/rooms/clear-all-and-queues') {
      // TESTING ONLY: Clear all rooms, reset index, and clear all queues
      if (!env.ROOM_INDEX) {
        return new Response(JSON.stringify({ error: 'no room index' }), { 
          status: 500, 
          headers: corsHeaders 
        });
      }
      
      try {
        // Get room IDs from the index instead of listing Durable Objects
        const indexId = env.ROOM_INDEX.idFromName('index');
        const indexObj = env.ROOM_INDEX.get(indexId);
        
        // Get all rooms from index
        const listRes = await indexObj.fetch(new Request('https://do/list', {
          method: 'GET',
          headers: { 'Content-Type': 'application/json' }
        }));
        const listData = await listRes.json().catch(() => ({ rooms: [] }));
        const rooms = listData.rooms || [];
        const roomIds = rooms.map(r => r.roomId).filter(Boolean);
        
        // Delete all room Durable Objects
        for (const roomId of roomIds) {
          if (!roomId) {
            console.log('Skipping undefined roomId');
            continue;
          }
          try {
            const roomObjId = env.GAME_ROOMS.idFromName(roomId);
            const roomObj = env.GAME_ROOMS.get(roomObjId);
            await roomObj.fetch(new Request('https://do/delete', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' }
            }));
          } catch (e) {
            console.log('Failed to delete room:', roomId, e);
          }
        }
        
        // Clear rooms from index
        await indexObj.fetch(new Request('https://do/clear', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' }
        }));
        
        // Clear all queues
        await indexObj.fetch(new Request('https://do/clearQueue', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({})
        }));
        
        return new Response(JSON.stringify({ 
          ok: true, 
          message: `All rooms cleared (${roomIds.length} rooms) and all queues cleared` 
        }), { 
          headers: Object.assign({ 'Content-Type': 'application/json' }, corsHeaders) 
        });
      } catch (e) {
        return new Response(JSON.stringify({ error: 'Failed to clear rooms and queues', details: e.message }), { 
          status: 500, 
          headers: corsHeaders 
        });
      }
    }

    if (request.method === 'POST' && url.pathname === '/rooms/clear-all') {
      // TESTING ONLY: Clear all rooms and reset index
      if (!env.ROOM_INDEX) {
        return new Response(JSON.stringify({ error: 'no room index' }), { 
          status: 500, 
          headers: corsHeaders 
        });
      }
      
      try {
        // Get all rooms from index
        const indexId = env.ROOM_INDEX.idFromName('index');
        const indexObj = env.ROOM_INDEX.get(indexId);
        const listRes = await indexObj.fetch(new Request('https://do/list'));
        const listData = await listRes.json().catch(() => ({ rooms: [] }));
        const rooms = listData.rooms || [];
        
        // Delete each room's Durable Object storage
        for (const room of rooms) {
          try {
            const roomId = room.roomId;
            const objId = env.GAME_ROOMS.idFromName(roomId);
            const obj = env.GAME_ROOMS.get(objId);
            await obj.fetch(new Request('https://do/delete', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' }
            }));
          } catch (e) {
            console.error('Failed to delete room:', room.roomId, e);
          }
        }
        
        // Clear the room index
        await indexObj.fetch(new Request('https://do/clear', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' }
        }));
        
        return new Response(JSON.stringify({ ok: true, message: `All rooms cleared (${rooms.length} rooms deleted)` }), { 
          headers: Object.assign({ 'Content-Type': 'application/json' }, corsHeaders) 
        });
      } catch (e) {
        return new Response(JSON.stringify({ error: 'Failed to clear rooms', details: e.message }), { 
          status: 500, 
          headers: corsHeaders 
        });
      }
    }

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
            mainTimeMs: body.mainTimeMs,
            private: body.private || false
          }),
          headers: { 'Content-Type': 'application/json' }
        });
        const res = await obj.fetch(initReq);
        const data = await res.json();
        return new Response(JSON.stringify({ ok: true, roomId, meta: data }), { headers: Object.assign({ 'Content-Type': 'application/json' }, corsHeaders) });
      }

    if (request.method === 'POST' && url.pathname === '/rooms/join-next') {
      const body = await request.json().catch(() => ({}));
      const playerId = body.playerId;
      const name = body.name || null;
      const mainTimeMs = body.mainTimeMs || 10 * 60 * 1000; // Default to 10 minutes
      
      if (!env.ROOM_INDEX) {
        return new Response(JSON.stringify({ error: 'no_matchmaking' }), { 
          status: 500, 
          headers: corsHeaders 
        });
      }
      
      const idxId = env.ROOM_INDEX.idFromName('index');
      const idxObj = env.ROOM_INDEX.get(idxId);
      const listRes = await idxObj.fetch(new Request('https://do/list'));
      const listData = await listRes.json().catch(() => ({ rooms: [] }));
      const rooms = listData.rooms || [];

      const candidates = rooms.filter(r => 
        r.phase === 'LOBBY' && 
        (r.players?.length || 0) < 2 &&
        !r.private &&  // Exclude private rooms from quick match
        r.mainTimeMs === mainTimeMs  // Match time control
      );
      if (candidates.length === 0) {
        return new Response(JSON.stringify({ error: 'no_lobby_rooms' }), { 
          status: 404, 
          headers: corsHeaders 
        });
      }

      const pick = candidates[0];
      const roomId = pick.roomId;
      const objId = env.GAME_ROOMS.idFromName(roomId);
      const obj = env.GAME_ROOMS.get(objId);
      const joinReq = new Request('https://do/joinRoom', { 
        method: 'POST', 
        headers: { 'Content-Type': 'application/json' }, 
        body: JSON.stringify({ playerId, name }) 
      });
      const joinRes = await obj.fetch(joinReq);
      const joinData = await joinRes.json().catch(() => ({}));
      return new Response(JSON.stringify({ ok: true, room: joinData.room || joinData }), { 
        headers: Object.assign({ 'Content-Type': 'application/json' }, corsHeaders) 
      });
    }

    if (request.method === 'GET' && url.pathname === '/rooms/available-count') {
      if (!env.ROOM_INDEX) {
        return new Response(JSON.stringify({ count: 0 }), { 
          headers: Object.assign({ 'Content-Type': 'application/json' }, corsHeaders) 
        });
      }
      
      const idxId = env.ROOM_INDEX.idFromName('index');
      const idxObj = env.ROOM_INDEX.get(idxId);
      const listRes = await idxObj.fetch(new Request('https://do/list'));
      const listData = await listRes.json().catch(() => ({ rooms: [] }));
      const rooms = listData.rooms || [];

      const availableRooms = rooms.filter(r => 
        r.phase === 'LOBBY' && 
        (r.players?.length || 0) < 2 &&
        !r.private  // Exclude private rooms
      );
      
      return new Response(JSON.stringify({ count: availableRooms.length }), { 
        headers: Object.assign({ 'Content-Type': 'application/json' }, corsHeaders) 
      });
    }

    // Queue endpoints
    if (request.method === 'POST' && url.pathname === '/queue/joinAll') {
      const body = await request.json().catch(() => ({}));
      const { playerId, name } = body;
      
      if (!env.ROOM_INDEX) {
        return new Response(JSON.stringify({ error: 'no_queue_system' }), { 
          status: 500, 
          headers: corsHeaders 
        });
      }
      
      // SIMPLIFIED: Join all 3 queues using the working single queue logic
      const idxId = env.ROOM_INDEX.idFromName('index');
      const idxObj = env.ROOM_INDEX.get(idxId);
      
      const timeControls = [5 * 60 * 1000, 10 * 60 * 1000, 15 * 60 * 1000]; // 5, 10, 15 minutes
      let matchFound = false;
      let matchData = null;
      
      for (const mainTimeMs of timeControls) {
        if (matchFound) break;
        
        const joinReq = new Request('https://do/addToQueue', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ 
            playerId, 
            name, 
            mainTimeMs
          })
        });
        
        const response = await idxObj.fetch(joinReq);
        const data = await response.json();
        
        if (data.shouldCreateRoom && data.queuedPlayers && data.queuedPlayers.length >= 2) {
          matchFound = true;
          matchData = { ...data, mainTimeMs };
          break;
        }
      }
      
      if (matchFound) {
        const roomId = `room-${crypto.randomUUID()}`;
        const roomObjId = env.GAME_ROOMS.idFromName(roomId);
        const roomObj = env.GAME_ROOMS.get(roomObjId);
        
        const initReq = new Request('https://do/initRoom', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            roomId,
            private: false,
            mainTimeMs: matchData.mainTimeMs,
            queuedPlayers: matchData.queuedPlayers
          })
        });
        
        const initResponse = await roomObj.fetch(initReq);
        const initResult = await initResponse.json();
        
        if (!initResult.ok) {
          return new Response(JSON.stringify({ 
            error: 'Room initialization failed', 
            details: initResult 
          }), { 
            status: 500,
            headers: Object.assign({ 'Content-Type': 'application/json' }, corsHeaders) 
          });
        }
        
        const playerIdsToRemove = matchData.queuedPlayers.map(p => p.playerId);
        
        const removeReq = new Request('https://do/removeFromAllQueues', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ 
            playerIds: playerIdsToRemove 
          })
        });
        
        await idxObj.fetch(removeReq);
        
        return new Response(JSON.stringify({ 
          ok: true, 
          roomId, 
          meta: initResult 
        }), { 
          headers: Object.assign({ 'Content-Type': 'application/json' }, corsHeaders) 
        });
      }
      
      return new Response(JSON.stringify({ 
        ok: true, 
        queued: true,
        joinedQueues: [5, 10, 15]
      }), { 
        headers: Object.assign({ 'Content-Type': 'application/json' }, corsHeaders) 
      });
    }

    if (request.method === 'POST' && url.pathname === '/queue/join') {
      const body = await request.json().catch(() => ({}));
      const { playerId, name, mainTimeMs } = body;
      
      if (!env.ROOM_INDEX) {
        return new Response(JSON.stringify({ error: 'no_queue_system' }), { 
          status: 500, 
          headers: corsHeaders 
        });
      }
      
      const idxId = env.ROOM_INDEX.idFromName('index');
      const idxObj = env.ROOM_INDEX.get(idxId);
      
      // Add player to queue
      const queueReq = new Request('https://do/addToQueue', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ playerId, name, mainTimeMs })
      });
      
      const queueRes = await idxObj.fetch(queueReq);
      const queueData = await queueRes.json();
      
      // Check if we should create a room (2 players in queue)
      if (queueData.shouldCreateRoom) {
        // Create room directly using internal method
        const roomId = `room-${crypto.randomUUID()}`;
        const objId = env.GAME_ROOMS.idFromName(roomId);
        const obj = env.GAME_ROOMS.get(objId);
        
        const createReq = new Request('https://do/initRoom', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ 
            roomId,
            private: false, 
            mainTimeMs: queueData.mainTimeMs,
            queuedPlayers: queueData.queuedPlayers
          }),
        });
        
        const createRes = await obj.fetch(createReq);
        
        if (createRes.ok) {
          const createData = await createRes.json();
          const roomId = createData.roomId || createData.meta?.roomId;
          
          // Remove matched players from ALL queues
          const removeReq = new Request('https://do/removeFromAllQueues', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ 
              playerIds: queueData.queuedPlayers.map(p => p.playerId) 
            })
          });
          await idxObj.fetch(removeReq);
          
          return new Response(JSON.stringify({ 
            ok: true, 
            roomId,
            room: createData.room || createData 
          }), { 
            headers: Object.assign({ 'Content-Type': 'application/json' }, corsHeaders) 
          });
        }
      }
      
      return new Response(JSON.stringify({ 
        ok: true, 
        queued: true,
        queuePosition: queueData.queuePosition 
      }), { 
        headers: Object.assign({ 'Content-Type': 'application/json' }, corsHeaders) 
      });
    }

    if (request.method === 'POST' && url.pathname === '/queue/leave') {
      const body = await request.json().catch(() => ({}));
      const { playerId } = body;
      
      if (!env.ROOM_INDEX) {
        return new Response(JSON.stringify({ error: 'no_queue_system' }), { 
          status: 500, 
          headers: corsHeaders 
        });
      }
      
      const idxId = env.ROOM_INDEX.idFromName('index');
      const idxObj = env.ROOM_INDEX.get(idxId);
      
      // Remove player from all queues
      const leaveReq = new Request('https://do/removeFromAllQueues', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ playerIds: [playerId] })
      });
      
      await idxObj.fetch(leaveReq);
      
      return new Response(JSON.stringify({ ok: true }), { 
        headers: Object.assign({ 'Content-Type': 'application/json' }, corsHeaders) 
      });
    }

    if (request.method === 'POST' && url.pathname === '/queue/clear-all-queues') {
      if (!env.ROOM_INDEX) {
        return new Response(JSON.stringify({ error: 'no_queue_system' }), {
          status: 500,
          headers: corsHeaders
        });
      }

      const idxId = env.ROOM_INDEX.idFromName('index');
      const idxObj = env.ROOM_INDEX.get(idxId);

      const clearReq = new Request('https://do/clear-all-queues', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({})
      });

      const response = await idxObj.fetch(clearReq);
      const data = await response.json();

      return new Response(JSON.stringify(data), {
        headers: Object.assign({ 'Content-Type': 'application/json' }, corsHeaders)
      });
    }

    if (request.method === 'POST' && url.pathname === '/queue/updateClocks') {
      if (!env.ROOM_INDEX) {
        return new Response(JSON.stringify({ error: 'no_queue_system' }), {
          status: 500,
          headers: corsHeaders
        });
      }

      const idxId = env.ROOM_INDEX.idFromName('index');
      const idxObj = env.ROOM_INDEX.get(idxId);

      const updateReq = new Request('https://do/updateClocks', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: request.body
      });

      const data = await idxObj.fetch(updateReq).then(r => r.json());
      return new Response(JSON.stringify(data), {
        headers: Object.assign({ 'Content-Type': 'application/json' }, corsHeaders)
      });
    }

    if (request.method === 'POST' && url.pathname === '/queue/debugEstimates') {
      if (!env.ROOM_INDEX) {
        return new Response(JSON.stringify({ error: 'no_queue_system' }), {
          status: 500,
          headers: corsHeaders
        });
      }

      const idxId = env.ROOM_INDEX.idFromName('index');
      const idxObj = env.ROOM_INDEX.get(idxId);

      const debugReq = new Request('https://do/debugEstimates', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      });

      const data = await idxObj.fetch(debugReq).then(r => r.json());
      return new Response(JSON.stringify(data), {
        headers: Object.assign({ 'Content-Type': 'application/json' }, corsHeaders)
      });
    }

    if (request.method === 'POST' && url.pathname === '/queue/debugQueues') {
      if (!env.ROOM_INDEX) {
        return new Response(JSON.stringify({ error: 'no_queue_system' }), {
          status: 500,
          headers: corsHeaders
        });
      }

      const idxId = env.ROOM_INDEX.idFromName('index');
      const idxObj = env.ROOM_INDEX.get(idxId);

      const debugReq = new Request('https://do/debugQueues', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      });

      const response = await idxObj.fetch(debugReq);
      const data = await response.json();

      return new Response(JSON.stringify(data), {
        headers: Object.assign({ 'Content-Type': 'application/json' }, corsHeaders)
      });
    }

    if (request.method === 'POST' && url.pathname === '/queue/debugRooms') {
      if (!env.ROOM_INDEX) {
        return new Response(JSON.stringify({ error: 'no_queue_system' }), {
          status: 500,
          headers: corsHeaders
        });
      }

      const idxId = env.ROOM_INDEX.idFromName('index');
      const idxObj = env.ROOM_INDEX.get(idxId);

      const debugReq = new Request('https://do/debugRooms', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      });

      const response = await idxObj.fetch(debugReq);
      const data = await response.json();

      return new Response(JSON.stringify(data), {
        headers: Object.assign({ 'Content-Type': 'application/json' }, corsHeaders)
      });
    }

    if (request.method === 'POST' && url.pathname === '/queue/checkMatch') {
      const body = await request.json().catch(() => ({}));
      const { playerId } = body;
      
      if (!env.ROOM_INDEX) {
        return new Response(JSON.stringify({ error: 'no_queue_system' }), { 
          status: 500, 
          headers: corsHeaders 
        });
      }
      
      const idxId = env.ROOM_INDEX.idFromName('index');
      const idxObj = env.ROOM_INDEX.get(idxId);
      
      const checkReq = new Request('https://do/checkMatch', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ playerId })
      });
      
      const response = await idxObj.fetch(checkReq);
      const data = await response.json();
      
      return new Response(JSON.stringify(data), { 
        headers: Object.assign({ 'Content-Type': 'application/json' }, corsHeaders) 
      });
    }

    if (request.method === 'POST' && url.pathname === '/queue/heartbeat') {
      const body = await request.json().catch(() => ({}));
      const { playerId } = body;
      
      if (!env.ROOM_INDEX) {
        return new Response(JSON.stringify({ error: 'no_queue_system' }), { 
          status: 500, 
          headers: corsHeaders 
        });
      }
      
      const idxId = env.ROOM_INDEX.idFromName('index');
      const idxObj = env.ROOM_INDEX.get(idxId);
      
      // Send heartbeat to update player's last activity
      const heartbeatReq = new Request('https://do/heartbeat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ playerId })
      });
      
      await idxObj.fetch(heartbeatReq);
      
      return new Response(JSON.stringify({ ok: true }), { 
        headers: Object.assign({ 'Content-Type': 'application/json' }, corsHeaders) 
      });
    }

    if (request.url.endsWith('/queue/ws') && request.headers.get('Upgrade') === 'websocket') {
      if (!env.ROOM_INDEX) {
        return new Response(JSON.stringify({ error: 'no_queue_system' }), { 
          status: 500, 
          headers: corsHeaders 
        });
      }
      
      const idxId = env.ROOM_INDEX.idFromName('index');
      const idxObj = env.ROOM_INDEX.get(idxId);
      
      return idxObj.fetch(request);
    }

    if (request.method === 'GET' && url.pathname === '/queue/status') {
      if (!env.ROOM_INDEX) {
        return new Response(JSON.stringify({ error: 'no_queue_system' }), { 
          status: 500, 
          headers: corsHeaders 
        });
      }
      
      const idxId = env.ROOM_INDEX.idFromName('index');
      const idxObj = env.ROOM_INDEX.get(idxId);
      
      // Get queue status with estimated wait times
      const statusReq = new Request('https://do/queue-status', {
        method: 'GET',
        headers: { 'Content-Type': 'application/json' }
      });
      
      const statusRes = await idxObj.fetch(statusReq);
      const statusData = await statusRes.json();
      
      return new Response(JSON.stringify(statusData), { 
        headers: Object.assign({ 'Content-Type': 'application/json' }, corsHeaders) 
      });
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
          const response = await obj.fetch(new Request('https://do/joinRoom', { method: 'POST', headers, body: bodyText }));
          const responseData = await response.json().catch(() => ({}));
          return new Response(JSON.stringify(responseData), {
            status: response.status,
            headers: Object.assign({}, corsHeaders, {
              'Content-Type': 'application/json'
            })
          });
        }
        if (segments.length === 3 && segments[2] === 'start-bidding' && request.method === 'POST') {
          const bodyText = await request.clone().text().catch(() => null);
          const headers = { 'Content-Type': request.headers.get('Content-Type') || 'application/json' };
          const response = await obj.fetch(new Request('https://do/startBidding', { method: 'POST', headers, body: bodyText }));
          const responseData = await response.json().catch(() => ({}));
          return new Response(JSON.stringify(responseData), {
            status: response.status,
            headers: Object.assign({}, corsHeaders, {
              'Content-Type': 'application/json'
            })
          });
        }
        if (segments.length === 3 && segments[2] === 'submit-bid' && request.method === 'POST') {
          const bodyText = await request.clone().text().catch(() => null);
          const headers = { 'Content-Type': request.headers.get('Content-Type') || 'application/json' };
          const response = await obj.fetch(new Request('https://do/submitBid', { method: 'POST', headers, body: bodyText }));
          const responseData = await response.json().catch(() => ({}));
          return new Response(JSON.stringify(responseData), {
            status: response.status,
            headers: Object.assign({}, corsHeaders, {
              'Content-Type': 'application/json'
            })
          });
        }
        if (segments.length === 3 && segments[2] === 'choose-color' && request.method === 'POST') {
          const bodyText = await request.clone().text().catch(() => null);
          const headers = { 'Content-Type': request.headers.get('Content-Type') || 'application/json' };
          const response = await obj.fetch(new Request('https://do/chooseColor', { method: 'POST', headers, body: bodyText }));
          const responseData = await response.json().catch(() => ({}));
          return new Response(JSON.stringify(responseData), {
            status: response.status,
            headers: Object.assign({}, corsHeaders, {
              'Content-Type': 'application/json'
            })
          });
        }
        if (segments.length === 3 && segments[2] === 'move' && request.method === 'POST') {
          const bodyText = await request.clone().text().catch(() => null);
          const headers = { 'Content-Type': request.headers.get('Content-Type') || 'application/json' };
          const response = await obj.fetch(new Request('https://do/makeMove', { method: 'POST', headers, body: bodyText }));
          const responseData = await response.json().catch(() => ({}));
          return new Response(JSON.stringify(responseData), {
            status: response.status,
            headers: Object.assign({}, corsHeaders, {
              'Content-Type': 'application/json'
            })
          });
        }
        if (segments.length === 3 && segments[2] === 'time-forfeit' && request.method === 'POST') {
          const bodyText = await request.clone().text().catch(() => null);
          const headers = { 'Content-Type': request.headers.get('Content-Type') || 'application/json' };
          const response = await obj.fetch(new Request('https://do/timeForfeit', { method: 'POST', headers, body: bodyText }));
          const responseData = await response.json().catch(() => ({}));
          return new Response(JSON.stringify(responseData), {
            status: response.status,
            headers: Object.assign({}, corsHeaders, {
              'Content-Type': 'application/json'
            })
          });
        }
        if (segments.length === 3 && segments[2] === 'rematch' && request.method === 'POST') {
          const bodyText = await request.clone().text().catch(() => null);
          const headers = { 'Content-Type': request.headers.get('Content-Type') || 'application/json' };
          const response = await obj.fetch(new Request('https://do/rematch', { method: 'POST', headers, body: bodyText }));
          const responseData = await response.json().catch(() => ({}));
          return new Response(JSON.stringify(responseData), {
            status: response.status,
            headers: Object.assign({}, corsHeaders, {
              'Content-Type': 'application/json'
            })
          });
        }
        if (segments.length === 3 && segments[2] === 'leave' && request.method === 'POST') {
          const bodyText = await request.clone().text().catch(() => null);
          const headers = { 'Content-Type': request.headers.get('Content-Type') || 'application/json' };
          const response = await obj.fetch(new Request('https://do/leaveRoom', { method: 'POST', headers, body: bodyText }));
          const responseData = await response.json().catch(() => ({}));
          return new Response(JSON.stringify(responseData), {
            status: response.status,
            headers: Object.assign({}, corsHeaders, {
              'Content-Type': 'application/json'
            })
          });
        }
        if (segments.length === 3 && segments[2] === 'heartbeat' && request.method === 'POST') {
          const bodyText = await request.clone().text().catch(() => null);
          const headers = { 'Content-Type': request.headers.get('Content-Type') || 'application/json' };
          const response = await obj.fetch(new Request('https://do/heartbeat', { method: 'POST', headers, body: bodyText }));
          const responseData = await response.json().catch(() => ({}));
          return new Response(JSON.stringify(responseData), {
            status: response.status,
            headers: Object.assign({}, corsHeaders, {
              'Content-Type': 'application/json'
            })
          });
        }

      }
      return new Response(JSON.stringify({ error: 'route_not_found' }), { status: 404, headers: corsHeaders });
    } catch (err) {
      return new Response(JSON.stringify({ error: err?.message || String(err) }), { status: 500, headers: corsHeaders });
    }
  }
};