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
        players: (this.room.players || []).length,
        private: this.room.private || false,
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
    if (request.method === 'POST' && request.url.endsWith('/delete')) {
      // Delete this room's storage
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
    
    // Add creator as first player if provided
    if (body.creatorName && body.creatorPlayerId) {
      this.room.players.push({ 
        id: body.creatorPlayerId, 
        name: body.creatorName, 
        joinedAt: now 
      });
    }
    
    await this._save();
    return this._response({ ok: true, roomId: this.room.roomId });
  }

  async _handleJoin(request) {
    if (this.room.updatedAt && (this._now() - this.room.updatedAt) > 30 * 60 * 1000) {
      // Remove old room from index
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
      this.room.rematchWindowEnds = this._now() + 60 * 1000;
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

    const moved = game.move({ from, to, promotion: promotion || 'q' });
    if (!moved) return this._response({ error: 'illegal_move' }, 400);

    this.room.gameFen = game.fen();
    this.room.moves.push({ by: playerId, move, at: now });
    this.room.clocks.lastTickAt = now;
    this.room.clocks.turn = this.room.clocks.turn === 'white' ? 'black' : 'white';

    if (game.isCheckmate()) {
      this.room.phase = 'FINISHED';
      this.room.winnerId = playerId;
      this.room.clocks.frozenAt = now;  
      this.room.rematchWindowEnds = this._now() + 60 * 1000;
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
    else if (game.isFiftyMoves()) drawReason = 'fifty_move_rule';

    if (drawReason) {
      this.room.phase = 'FINISHED';
      this.room.winnerId = null;
      this.room.clocks.frozenAt = now;
      this.room.rematchWindowEnds = this._now() + 60 * 1000;
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

    if (this.room.updatedAt && (now - this.room.updatedAt) > 30 * 60 * 1000) {
      // Remove expired room from index
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

    // Cleanup rematch window expiry for finished games
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
        
        // If someone voted Yes but timeout occurred, add them to matchmaking
        if (yesVotes.length > 0 && !anyNo) {
          // Add the Yes voters to matchmaking queue
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
    
    // Check if player already voted (votes are irreversible)
    if (typeof this.room.rematchVotes[playerId] !== 'undefined') {
      return this._response({ error: 'already_voted' }, 400);
    }
    
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

    // Check if any player voted No - if so, they should be sent back to lobby
    const anyNo = players.some(pid => this.room.rematchVotes[pid] === false);
    if (anyNo) {
      return this._response({ ok: true, rematchStarted: false, voteResult: 'no_vote', votes: this.room.rematchVotes });
    }

    // Check if only one player voted Yes and the other hasn't voted yet
    const yesVotes = players.filter(pid => this.room.rematchVotes[pid] === true);
    const noVotes = players.filter(pid => this.room.rematchVotes[pid] === false);
    
    if (yesVotes.length === 1 && noVotes.length === 0) {
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
    if (request.method === 'POST' && request.url.endsWith('/clear')) {
      await this._saveAll({});
      return new Response(JSON.stringify({ ok: true, message: 'All rooms cleared from index' }), { headers: { 'Content-Type': 'application/json' } });
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
        r.players < 2 &&
        !r.private  // Exclude private rooms from quick match
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
        r.players < 2 &&
        !r.private  // Exclude private rooms
      );
      
      return new Response(JSON.stringify({ count: availableRooms.length }), { 
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
        if (segments.length === 3 && segments[2] === 'time-forfeit' && request.method === 'POST') {
          const bodyText = await request.clone().text().catch(() => null);
          const headers = { 'Content-Type': request.headers.get('Content-Type') || 'application/json' };
          return obj.fetch(new Request('https://do/timeForfeit', { method: 'POST', headers, body: bodyText }));
        }
        if (segments.length === 3 && segments[2] === 'rematch' && request.method === 'POST') {
          const bodyText = await request.clone().text().catch(() => null);
          const headers = { 'Content-Type': request.headers.get('Content-Type') || 'application/json' };
          return obj.fetch(new Request('https://do/rematch', { method: 'POST', headers, body: bodyText }));
        }
        if (segments.length === 3 && segments[2] === 'leave' && request.method === 'POST') {
          const bodyText = await request.clone().text().catch(() => null);
          const headers = { 'Content-Type': request.headers.get('Content-Type') || 'application/json' };
          return obj.fetch(new Request('https://do/leaveRoom', { method: 'POST', headers, body: bodyText }));
        }
        if (segments.length === 3 && segments[2] === 'heartbeat' && request.method === 'POST') {
          const bodyText = await request.clone().text().catch(() => null);
          const headers = { 'Content-Type': request.headers.get('Content-Type') || 'application/json' };
          return obj.fetch(new Request('https://do/heartbeat', { method: 'POST', headers, body: bodyText }));
        }

      }
      return new Response(JSON.stringify({ error: 'route_not_found' }), { status: 404, headers: corsHeaders });
    } catch (err) {
      return new Response(JSON.stringify({ error: err?.message || String(err) }), { status: 500, headers: corsHeaders });
    }
  }
};