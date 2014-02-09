var async = require('async');
var _ = require('underscore');
var net = require('net');
var EventEmitter = require('events').EventEmitter;
var util = require('util');
var hiredis = require('hiredis');


function ReplyParser(options) {
	this.options = options || {};
	this.reset();
	EventEmitter.call(this);
}
util.inherits(ReplyParser, EventEmitter);
ReplyParser.prototype.reset = function () {
	this.reader = new hiredis.Reader({return_buffers: this.options.return_buffers || false});
};
ReplyParser.prototype.execute = function (data) {
	var reply;
	this.reader.feed(data);
	while (true) {
		try {
			reply = this.reader.get();
		} catch (err) {
			this.emit('error', err);
			break;
		}
		if (reply === undefined) break;
		if (reply && reply.constructor === Error) {
			this.emit('reply error', reply);
		} else {
			this.emit('reply', reply);
		}
	}
};


function Queue() {
	this.tail = [];
	this.head = [];
	this.offset = 0;
}
Queue.prototype.shift = function () {
	if (this.offset === this.head.length) {
		var tmp = this.head;
		tmp.length = 0;
		this.head = this.tail;
		this.tail = tmp;
		this.offset = 0;
		if (this.head.length === 0) return;
	}
	return this.head[this.offset++];
};
Queue.prototype.push = function (item) {
	return this.tail.push(item);
};
Queue.prototype.getLength = function () {
	return this.head.length - this.offset + this.tail.length;
};


function try_callback(client, callback, reply) {
	try {
		callback(null, reply);
	} catch (err) {
		if (process.domain) {
			process.domain.emit('error', err);
			process.domain.exit();
		} else {
			client.emit('error', err);
		}
	}
}
function reply_to_object(reply) {
	var obj = {}, j, jl, key, val;
	if (reply.length === 0) return null;
	for (j = 0, jl = reply.length; j < jl; j += 2) {
		key = reply[j].toString();
		val = reply[j + 1];
		obj[key] = val;
	}
	return obj;
}
function Command(command, args, callback) {
	this.command = command;
	this.args = args;
	this.callback = callback;
}
function to_array(args) {
	var len = args.length, arr = new Array(len), i;
	for (i = 0; i < len; i += 1) {
		arr[i] = args[i];
	}
	return arr;
}


var connection_id = 0;
function Client(stream, options) {
	this.stream = stream;
	this.options = options = options || {};
	this.connection_id = ++connection_id;
	this.connected = false;
	this.ready = false;
	this.connections = 0;
	this.should_buffer = false;
	this.command_queue_high_water = this.options.command_queue_high_water || 1000;
	this.command_queue_low_water = this.options.command_queue_low_water || 0;
	this.max_attempts = null;
	if (options.max_attempts && !isNaN(options.max_attempts) && options.max_attempts > 0) {
		this.max_attempts = +options.max_attempts;
	}
	this.command_queue = new Queue();
	this.offline_queue = new Queue();
	this.commands_sent = 0;
	this.connect_timeout = false;
	if (options.connect_timeout && !isNaN(options.connect_timeout) && options.connect_timeout > 0) {
		this.connect_timeout = +options.connect_timeout;
	}
	this.enable_offline_queue = true;
	if (typeof this.options.enable_offline_queue === 'boolean') {
		this.enable_offline_queue = this.options.enable_offline_queue;
	}
	this.retry_max_delay = null;
	if (options.retry_max_delay !== undefined && !isNaN(options.retry_max_delay) && options.retry_max_delay > 0) {
		this.retry_max_delay = options.retry_max_delay;
	}
	this.initialize_retry_vars();
	this.selected_db = null;
	this.old_state = null;
	var self = this;
	this.stream.on('connect', function () {
		self.on_connect();
	});
	this.stream.on('data', function (buffer_from_socket) {
		try {
			self.reply_parser.execute(buffer_from_socket);
		} catch (err) {
			self.emit('error', err);
		}
	});
	this.stream.on('error', function (msg) {
		var message = 'Connection to ' + self.host + ":" + self.port + ' failed - ' + msg.message;
		self.flush_and_error(message);
		self.connected = false;
		self.ready = false;
		self.emit('error', new Error(message));
		self.connection_gone('error');
	});
	this.stream.on('close', function () {
		self.connection_gone('close');
	});
	this.stream.on('end', function () {
		self.connection_gone('end');
	});
	this.stream.on('drain', function () {
		self.should_buffer = false;
		self.emit("drain");
	});
	EventEmitter.call(this);
}
util.inherits(Client, EventEmitter);
Client.prototype.initialize_retry_vars = function () {
	this.retry_timer = null;
	this.retry_totaltime = 0;
	this.retry_delay = 150;
	this.retry_backoff = 1.7;
	this.attempts = 1;
};
Client.prototype.flush_and_error = function (message) {
	var command_obj;
	var error = new Error(message);
	while (this.offline_queue.getLength() > 0) {
		command_obj = this.offline_queue.shift();
		if (typeof command_obj.callback === 'function') {
			try {
				command_obj.callback(error);
			} catch (callback_err) {
				this.emit('error', callback_err);
			}
		}
	}
	this.offline_queue = new Queue();
	while (this.command_queue.getLength() > 0) {
		command_obj = this.command_queue.shift();
		if (typeof command_obj.callback === 'function') {
			try {
				command_obj.callback(error);
			} catch (callback_err) {
				this.emit('error', callback_err);
			}
		}
	}
	this.command_queue = new Queue();
};
Client.prototype.on_connect = function () {
	this.connected = true;
	this.ready = false;
	this.connections += 1;
	this.command_queue = new Queue();
	this.emitted_end = false;
	this.stream.setNoDelay();
	this.stream.setTimeout(0);
	var self = this;
	this.reply_parser = new ReplyParser({return_buffers: self.options.return_buffers || false});
	this.reply_parser.on('reply error', function (reply) {
		if (reply instanceof Error) {
			self.return_error(reply);
		} else {
			self.return_error(new Error(reply));
		}
	});
	this.reply_parser.on('reply', function (reply) {
		self.return_reply(reply);
	});
	this.reply_parser.on('error', function (err) {
		self.emit('error', new Error('Reply parser error: ' + err.stack));
	});
	this.emit('connect');
	this.initialize_retry_vars();
	this.on_ready();
};
Client.prototype.on_ready = function () {
	var self = this;
	this.ready = true;
	if (this.old_state !== null) {
		this.selected_db = this.old_state.selected_db;
		this.old_state = null;
	}
	if (this.selected_db !== null) {
		this.send_command('select', [this.selected_db]);
	}
	var command_obj, buffered_writes = 0;
	while (this.offline_queue.getLength() > 0) {
		command_obj = this.offline_queue.shift();
		buffered_writes += !this.send_command(command_obj.command, command_obj.args, command_obj.callback);
	}
	this.offline_queue = new Queue();
	if (!buffered_writes) {
		this.should_buffer = false;
		this.emit('drain');
	}
	this.emit('ready');
};
Client.prototype.connection_gone = function (why) {
	var self = this;
	if (this.retry_timer) {
		return;
	}
	this.connected = false;
	this.ready = false;
	if (this.old_state === null) {
		this.old_state = {selected_db: this.selected_db};
		this.selected_db = null;
	}
	if (! this.emitted_end) {
		this.emit('end');
		this.emitted_end = true;
	}
	this.flush_and_error('Connection gone from ' + why + ' event.');
	var nextDelay = Math.floor(this.retry_delay * this.retry_backoff);
	if (this.retry_max_delay !== null && nextDelay > this.retry_max_delay) {
		this.retry_delay = this.retry_max_delay;
	} else {
		this.retry_delay = nextDelay;
	}
	if (this.max_attempts && this.attempts >= this.max_attempts) {
		this.retry_timer = null;
		return;
	}
	this.attempts += 1;
	this.emit('reconnecting', {
		delay: self.retry_delay,
		attempt: self.attempts
	});
	this.retry_timer = setTimeout(function () {
		self.retry_totaltime += self.retry_delay;
		if (self.connect_timeout && self.retry_totaltime >= self.connect_timeout) {
			self.retry_timer = null;
			return;
		}
		self.stream.connect(self.port, self.host);
		self.retry_timer = null;
	}, this.retry_delay);
};
Client.prototype.return_error = function (err) {
	var command_obj = this.command_queue.shift(), queue_len = this.command_queue.getLength();
	if (queue_len === 0) {
		this.command_queue = new Queue();
		this.emit('idle');
	}
	if (this.should_buffer && queue_len <= this.command_queue_low_water) {
		this.emit('drain');
		this.should_buffer = false;
	}
	if (command_obj && typeof command_obj.callback === 'function') {
		try {
			command_obj.callback(err);
		} catch (callback_err) {
			this.emit('error', callback_err);
		}
	} else {
		this.emit('error', err);
	}
};
Client.prototype.return_reply = function (reply) {
	var command_obj, len, type, timestamp, argindex, args, queue_len;
	if (Array.isArray(reply) && reply.length > 0 && reply[0]) {
		type = reply[0].toString();
	}
	command_obj = this.command_queue.shift();
	queue_len = this.command_queue.getLength();
	if (queue_len === 0) {
		this.command_queue = new Queue();
		this.emit('idle');
	}
	if (this.should_buffer && queue_len <= this.command_queue_low_water) {
		this.emit('drain');
		this.should_buffer = false;
	}
	if (command_obj) {
		if (typeof command_obj.callback === 'function') {
			if (reply && 'hscan' === command_obj.command.toLowerCase()) {
				reply = reply_to_object(reply);
			}
			try_callback(this, command_obj.callback, reply);
		}
	}
};
Client.prototype.send_command = function (command, args, callback) {
	var arg, command_obj, i, il, elem_count, stream = this.stream, command_str = '', buffered_writes = 0, last_arg_type;
	if (Array.isArray(args)) {
		if (typeof callback === 'function') {
		} else if (! callback) {
			last_arg_type = typeof args[args.length - 1];
			if (last_arg_type === 'function' || last_arg_type === 'undefined') {
				callback = args.pop();
			}
		}
	}
	if (callback && process.domain) callback = process.domain.bind(callback);
	var lcaseCommand = command.toLowerCase();
	if ((lcaseCommand === 'sadd' || lcaseCommand === 'srem') && args.length > 0 && Array.isArray(args[args.length - 1])) {
		args = args.slice(0, -1).concat(args[args.length - 1]);
	}
	if (command === 'set' || command === 'setex') {
		if (args[args.length - 1] === undefined || args[args.length - 1] === null) {
			var err = new Error('send_command: ' + command + ' value must not be undefined or null');
			return callback && callback(err);
		}
	}
	command_obj = new Command(command, args, callback);
	if (!this.ready || !stream.writable) {
		if (this.enable_offline_queue) {
			this.offline_queue.push(command_obj);
			this.should_buffer = true;
		} else {
			var not_writeable_error = new Error('send_command: stream not writeable. enable_offline_queue is false');
			if (command_obj.callback) {
				command_obj.callback(not_writeable_error);
			} else {
				throw not_writeable_error;
			}
		}
		return false;
	}
	this.command_queue.push(command_obj);
	this.commands_sent += 1;
	elem_count = args.length + 1;
	command_str = "*" + elem_count + "\r\n$" + command.length + "\r\n" + command + "\r\n";
	for (i = 0, il = args.length, arg; i < il; i += 1) {
		arg = args[i];
		if (typeof arg !== 'string') arg = String(arg);
		command_str += "$" + Buffer.byteLength(arg) + "\r\n" + arg + "\r\n";
	}
	buffered_writes += !stream.write(command_str);
	if (buffered_writes || this.command_queue.getLength() >= this.command_queue_high_water) {
		this.should_buffer = true;
	}
	return !this.should_buffer;
};
Client.prototype.select = function (db, callback) {
	var self = this;
	this.send_command('select', [db], function (err, res) {
		if (err === null) self.selected_db = db;
		if (typeof(callback) === 'function') {
			callback(err, res);
		}
	});
};
Client.prototype.hmget = function (arg1, arg2, arg3) {
	if (Array.isArray(arg2) && typeof arg3 === 'function') {
		return this.send_command('multi_hget', [arg1].concat(arg2), arg3);
	} else if (Array.isArray(arg1) && typeof arg2 === 'function') {
		return this.send_command('multi_hget', arg1, arg2);
	} else {
		return this.send_command('multi_hget', to_array(arguments));
	}
};
Client.prototype.hmset = function (args, callback) {
	var tmp_args, tmp_keys, i, il, key;
	if (Array.isArray(args) && typeof callback === 'function') {
		return this.send_command('multi_hset', args, callback);
	}
	args = to_array(arguments);
	if (typeof args[args.length - 1] === 'function') {
		callback = args[args.length - 1];
		args.length -= 1;
	} else {
		callback = null;
	}
	if (args.length === 2 && (typeof args[0] === 'string' || typeof args[0] === 'number') && typeof args[1] === 'object') {
		if (typeof args[0] === 'number') {
			args[0] = args[0].toString();
		}
		tmp_args = [ args[0] ];
		tmp_keys = Object.keys(args[1]);
		for (i = 0, il = tmp_keys.length; i < il ; i++) {
			key = tmp_keys[i];
			tmp_args.push(key);
			tmp_args.push(args[1][key]);
		}
		args = tmp_args;
	}
	return this.send_command('multi_hset', args, callback);
};
Client.prototype.multi = function (args) {
	return new Multi(this, args);
};


function Multi(client, args) {
	this._client = client;
	this.queue = [['MULTI']];
	if (Array.isArray(args)) {
		this.queue = this.queue.concat(args);
	}
}
Multi.prototype.hmset = function () {
	var args = to_array(arguments), tmp_args;
	if (args.length >= 2 && (typeof args[0] === 'string' || typeof args[0] === 'number') && typeof args[1] === 'object') {
		if (typeof args[0] === 'number') {
			args[0] = args[0].toString();
		}
		tmp_args = ['multi_hset', args[0]];
		Object.keys(args[1]).map(function (key) {
			tmp_args.push(key);
			tmp_args.push(args[1][key]);
		});
		if (args[2]) {
			tmp_args.push(args[2]);
		}
		args = tmp_args;
	} else {
		args.unshift('multi_hset');
	}
	this.queue.push(args);
	return this;
};
Multi.prototype.exec = function (callback) {
	var self = this;
	var errors = [];
	this.queue.forEach(function (args, index) {
		var command = args[0], obj;
		if (typeof args[args.length - 1] === 'function') {
			args = args.slice(1, -1);
		} else {
			args = args.slice(1);
		}
		if (args.length === 1 && Array.isArray(args[0])) {
			args = args[0];
		}
		if (command.toLowerCase() === 'multi_hset' && typeof args[1] === 'object') {
			obj = args.pop();
			Object.keys(obj).forEach(function (key) {
				args.push(key);
				args.push(obj[key]);
			});
		}
		this._client.send_command(command, args, function (err, reply) {
			if (err) {
				var cur = self.queue[index];
				if (typeof cur[cur.length - 1] === 'function') {
					cur[cur.length - 1](err);
				} else {
					errors.push(new Error(err));
				}
			}
		});
	}, this);
	return this._client.send_command('EXEC', [], function (err, replies) {
		if (err) {
			if (callback) {
				errors.push(new Error(err));
				callback(errors);
				return;
			} else {
				throw new Error(err);
			}
		}
		var i, il, reply, args;
		if (replies) {
			for (i = 1, il = self.queue.length; i < il; i += 1) {
				reply = replies[i - 1];
				args = self.queue[i];
				if (reply && args[0].toLowerCase() === 'hscan') {
					replies[i - 1] = reply = reply_to_object(reply);
				}
				if (typeof args[args.length - 1] === 'function') {
					args[args.length - 1](null, reply);
				}
			}
		}
		if (callback) {
			callback(null, replies);
		}
	});
};

["get", "set", "setnx", "setex", "append", "strlen", "del", "exists", "setbit", "getbit", "setrange", "getrange", "substr",
"incr", "decr", "mget", "rpush", "lpush", "rpushx", "lpushx", "linsert", "rpop", "lpop", "brpop", "brpoplpush", "blpop", "llen", "lindex",
"lset", "lrange", "ltrim", "lrem", "rpoplpush", "sadd", "srem", "smove", "sismember", "scard", "spop", "srandmember", "sinter", "sinterstore",
"sunion", "sunionstore", "sdiff", "sdiffstore", "smembers", "zadd", "zincrby", "zrem", "zremrangebyscore", "zremrangebyrank", "zunionstore",
"zinterstore", "zrange", "zrangebyscore", "zrevrangebyscore", "zcount", "zrevrange", "zcard", "zscore", "zrank", "zrevrank", "hset", "hsetnx",
"hget", "hmset", "hmget", "hincrby", "hdel", "hlen", "hkeys", "hvals", "hgetall", "hexists", "incrby", "decrby", "getset", "mset", "msetnx",
"randomkey", "select", "move", "rename", "renamenx", "expire", "expireat", "keys", "dbsize", "auth", "ping", "echo", "save", "bgsave",
"bgrewriteaof", "shutdown", "lastsave", "type", "multi", "exec", "discard", "sync", "flushdb", "flushall", "sort", "info", "monitor", "ttl",
"persist", "slaveof", "debug", "config", "publish", "watch", "unwatch", "cluster",
"restore", "migrate", "dump", "object", "client"].forEach(function (command) {
	Client.prototype[command] = function (args, callback) {
		if (Array.isArray(args) && typeof callback === 'function') {
			return this.send_command(command, args, callback);
		} else {
			return this.send_command(command, to_array(arguments));
		}
	};
	Multi.prototype[command] = function () {
		this.queue.push([command].concat(to_array(arguments)));
		return this;
	};
});

exports.createClient = function (port_arg, host_arg, options) {
	var port = port_arg || 6379;
	var host = host_arg || '127.0.0.1';
	var client = new Client(net.connect(port, host), options);
	client.port = port;
	client.host = host;
	return client;
};

















exports.connect = function(host, port, timeout, listener){
	var self = this;
	var callbacks = [];
	var connected = false;

	if(typeof(timeout) == 'function'){
		listener = timeout;
		timeout = 0;
	}
	listener = listener || function(){};

	var sock = new net.Socket();
	sock.on('error', function(e){
		if(!connected){
			listener('connect_failed', e);
		}else{
			var callback = callbacks.shift();
			callback(['error']);
		}
	});
	sock.connect(port, host, function(){
		connected = true;
		sock.setNoDelay(true);
		sock.setKeepAlive(true);
		sock.setTimeout(timeout);
		listener(0, self);
	});

	self.close = function(){
		sock.end();
	}

	self.request = function(cmd, params, callback){
		var arr = [cmd].concat(params);
		self.send_request(arr);
		callbacks.push(callback || function(){});
	}

	function build_buffer(arr){
		var bs = [];
		var size = 0;
		for(var i = 0; i < arr.length; i++){
			var arg = arr[i];
			if(arg instanceof Buffer){
				//
			}else{
				arg = new Buffer(arg.toString());
			}
			bs.push(arg);
			size += arg.length;
		}
		var ret = new Buffer(size);
		var offset = 0;
		for(var i=0; i<bs.length; i++){
			bs[i].copy(ret, offset);
			offset += bs[i].length;
		}
		return ret;
	}
	
	self.send_request = function(params){
		var bs = [];
		for(var i in params){
			var p = params[i];
			var len = 0;
			if(!(p instanceof Buffer)){
				p = p.toString();
			}
			bs.push(Buffer.byteLength(p));
			bs.push('\n');
			bs.push(p);
			bs.push('\n');
		}
		bs.push('\n');
		var req = build_buffer(bs);
		sock.write(req);
		//console.log('write ' + req.length + ' bytes');
		//console.log('write: ' + req);
	}
	var recv_buf = new Buffer(0);
	sock.on('data', function(data){
		recv_buf = build_buffer([recv_buf, data]);
		while(recv_buf.length > 0){
			var resp = parse();
			if(!resp){
				break;
			}
			resp[0] = resp[0].toString();
			var callback = callbacks.shift();
			callback(resp);
		}
	});

	function memchr(buf, ch, start){
		start = start || 0;
		ch = typeof(ch) == 'string'? ch.charCodeAt(0) : ch;
		for(var i=start; i<buf.length; i++){
			if(buf[i] == ch){
				return i;
			}
		}
		return -1;
	}

	function parse(){
		var ret = [];
		var spos = 0;
		var pos;
		//console.log('parse: ' + recv_buf.length + ' bytes');
		while(true){
			//pos = recv_buf.indexOf('\n', spos);
			pos = memchr(recv_buf, '\n', spos);
			if(pos == -1){
				// not finished
				return null;
			}
			var line = recv_buf.slice(spos, pos).toString();
			spos = pos + 1;
			line = line.replace(/^\s+(.*)\s+$/, '\1');
			if(line.length == 0){
				// parse end
				//recv_buf = recv_buf.substr(spos);
				recv_buf = recv_buf.slice(spos);
				break;
			}
			var len = parseInt(line);
			if(isNaN(len)){
				// error
				console.log('error 1');
				return null;
			}
			if(spos + len > recv_buf.length){
				// not finished
				//console.log(spos + len, recv_buf.length);
				//console.log('not finish');
				return null;
			}
			//var data = recv_buf.substr(spos, len);
			var data = recv_buf.slice(spos, spos + len);
			spos += len;
			ret.push(data);

			//pos = recv_buf.indexOf('\n', spos);
			pos = memchr(recv_buf, '\n', spos);
			if(pos == -1){
				// not finished
				console.log('error 3');
				return null;
			}
			// '\n', or '\r\n'
			//if(recv_buf.charAt(spos) != '\n' && recv_buf.charAt(spos) != '\r' && recv_buf.charAt(spos+1) != '\n'){
			var cr = '\r'.charCodeAt(0);
			var lf = '\n'.charCodeAt(0);
			if(recv_buf[spos] != lf && recv_buf[spos] != cr && recv_buf[spos+1] != lf){
				// error
				console.log('error 4 ' + recv_buf[spos]);
				return null;
			}
			spos = pos + 1;
		}
		return ret;
	}

	// callback(err, val);
	// err: 0 on sucess, or error_code(string) on error
	self.get = function(key, callback){
		self.request('get', [key], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				var val = resp[1];
				callback(err, val);
			}
		});
	}

	// callback(err);
	self.set = function(key, val, callback){
		self.request('set', [key, val], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				callback(err);
			}
		});
	}

	// callback(err);
	self.del = function(key, callback){
		self.request('del', [key], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				callback(err);
			}
		});
	}

	// callback(err, {index:[], items:{key:score}})
	self.scan = function(key_start, key_end, limit, callback){
		self.request('scan', [key_start, key_end, limit], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				if(resp.length % 2 != 1){
					callback('error');
				}else{
					var data = {index: [], items: {}};
					for(var i=1; i<resp.length; i+=2){
						var k = resp[i].toString();
						var v = resp[i+1].toString();
						data.index.push(k);
						data.items[k] = v;
					}
					callback(err, data);
				}
			}
		});
	}

	// callback(err, [])
	self.keys = function(key_start, key_end, limit, callback){
		self.request('keys', [key_start, key_end, limit], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				var data = [];
				for(var i=1; i<resp.length; i++){
					var k = resp[i].toString();
					data.push(k);
				}
				callback(err, data);
			}
		});
	}

	//////////////////////////////////////////////

	// callback(score)
	self.zget = function(name, key, callback){
		self.request('zget', [name, key], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				if(resp.length == 2){
					var score = parseInt(resp[1]);
					callback(err, score);
				}else{
					var score = 0;
					callback('error');
				}
			}
		});
	}

	// callback(size)
	self.zsize = function(name, callback){
		self.request('zsize', [name], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				if(resp.length == 2){
					var size = parseInt(resp[1]);
					callback(err, size);
				}else{
					var score = 0;
					callback('error');
				}
			}
		});
	}

	// callback(err);
	self.zset = function(name, key, score, callback){
		self.request('zset', [name, key, score], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				callback(err);
			}
		});
	}

	// callback(err);
	self.zdel = function(name, key, callback){
		self.request('zdel', [name, key], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				callback(err);
			}
		});
	}

	// callback(err, {index:[], items:{key:score}})
	self.zscan = function(name, key_start, score_start, score_end, limit, callback){
		self.request('zscan', [name, key_start, score_start, score_end, limit], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				if(resp.length % 2 != 1){
					callback('error');
				}else{
					var data = {index: [], items: {}};
					for(var i=1; i<resp.length; i+=2){
						var k = resp[i].toString();
						var v = parseInt(resp[i+1]);
						data.index.push(k);
						data.items[k] = v;
					}
					callback(err, data);
				}
			}
		});
	}

	// callback(err, [])
	self.zlist = function(name_start, name_end, limit, callback){
		self.request('zlist', [name_start, name_end, limit], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				var data = [];
				for(var i=1; i<resp.length; i++){
					var k = resp[i].toString();
					data.push(k);
				}
				callback(err, data);
			}
		});
	}

	//////////////////////////////////////////////

	// callback(val)
	self.hget = function(name, key, callback){
		self.request('hget', [name, key], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				if(resp.length == 2){
					callback(err, resp[1]);
				}else{
					callback('error');
				}
			}
		});
	}

	// callback(err);
	self.hset = function(name, key, val, callback){
		self.request('hset', [name, key, val], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				callback(err);
			}
		});
	}

	// callback(err);
	self.hdel = function(name, key, callback){
		self.request('hdel', [name, key], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				callback(err);
			}
		});
	}

	// callback(err, {index:[], items:{key:score}})
	self.hscan = function(name, key_start, key_end, limit, callback){
		self.request('hscan', [name, key_start, key_end, limit], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				if(resp.length % 2 != 1){
					callback('error');
				}else{
					var data = {index: [], items: {}};
					for(var i=1; i<resp.length; i+=2){
						var k = resp[i].toString();
						var v = resp[i+1].toString();
						data.index.push(k);
						data.items[k] = v;
					}
					callback(err, data);
				}
			}
		});
	}

	// callback(err, [])
	self.hlist = function(name_start, name_end, limit, callback){
		self.request('hlist', [name_start, name_end, limit], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				var data = [];
				for(var i=1; i<resp.length; i++){
					var k = resp[i].toString();
					data.push(k);
				}
				callback(err, data);
			}
		});
	}

	// callback(size)
	self.hsize = function(name, callback){
		self.request('hsize', [name], function(resp){
			if(callback){
				var err = resp[0] == 'ok'? 0 : resp[0];
				if(resp.length == 2){
					var size = parseInt(resp[1]);
					callback(err, size);
				}else{
					var score = 0;
					callback('error');
				}
			}
		});
	}

	return self;
}


/*
example:
var SSDB = require('./SSDB.js');
var ssdb = SSDB.connect(host, port, function(err){
	if(err){
		return;
	}
	ssdb.set('a', new Date(), function(){
		console.log('set a');
	});
});
*/














exports.initialize = function (schema, callback) {
	if (!ssdb) return;
	if (schema.settings.url) {
		var url = require('url');
		var ssdbUrl = url.parse(schema.settings.url);
		schema.settings.host = ssdbUrl.hostname;
		schema.settings.port = ssdbUrl.port;
	}
	schema.client = redis.createClient(
		schema.settings.port,
		schema.settings.host,
		schema.settings.options
	);
	schema.client.auth(schema.settings.password);
	var callbackCalled = false;
	var database = schema.settings.hasOwnProperty('database') && schema.settings.database;
	schema.client.on('connect', function () {
		if (!callbackCalled && database === false) {
			callbackCalled = true;
			callback();
		} else if (database !== false) {
			if (callbackCalled) {
				return schema.client.select(schema.settings.database);
			} else {
				callbackCalled = true;
				return schema.client.select(schema.settings.database, callback);
			}
		}
	});
	schema.client.on('error', function (error) {
		console.log(error);
	})
	schema.adapter = new SSDB(schema.client);
};
function SSDB(client) {
	this.name = 'ssdb';
	this.client = client;
	this._models = {};
	this.indexes = {};
}
SSDB.prototype.forDb = function (model, data) {
	var properties = this._models[model].properties;
	for (var prop in data) {
		if (!properties[prop]) continue;
		switch (properties[prop].type.name) {
			case 'Date':
				if (data[prop].getTime) {
					data[prop] = data[prop].getTime().toString();
				} else if (typeof data[prop] === 'number') {
					data[prop] = parseInt(data[prop], 10).toString();
				} else if (typeof data[prop] === 'string' && !isNaN(parseInt(data[prop]))) {
				} else {
					delete data[prop];
				}
				break;
			case 'Number':
				data[prop] = Number(data[prop]);
				if (!isNaN(data[prop])) {
					data[prop] = data[prop].toString();
				} else {
					delete data[prop];
				}
				break;
			case 'Boolean':
				data[prop] = data[prop] ? '1' : '0';
				break;
			case 'String':
			case 'Text':
				if (typeof data[prop] !== 'string') {
					delete data[prop];
				}
				break;
			default:
				data[prop] = JSON.stringify(data[prop]);
		}
	}
	return data;
};
SSDB.prototype.fromDb = function (model, data) {
	var properties = this._models[model].properties;
	for (var prop in data) {
		if (!properties[prop]) continue;
		switch (properties[prop].type.name) {
			case 'Date':
				data[prop] = new Date(parseInt(data[prop], 10));
				break;
			case 'Number':
				data[prop] = Number(data[prop]);
				break;
			case 'Boolean':
				data[prop] = data[prop] === '1';
				break;
			case 'String':
			case 'Text':
				break;
			default:
				try {
					data[prop] = JSON.parse(data[prop]);
				} catch(e) {}
		}
	}
	return data;
};
SSDB.prototype.id = function () {
	var now;
	try { now = require('microtime').now; } catch (e) {}
	if (!now) {
		now = function () { return Date.now() * 1000; };
	}
	return now().toString(36) + Math.floor(Math.random() * 36).toString(36);
};
SSDB.prototype.define = function (descr) {
	var model = descr.model.modelName;
	if (!descr.settings) descr.settings = {};
	this._models[model] = descr;
	this.indexes[model] = {};
	Object.keys(descr.properties).forEach(function (prop) {
		if (descr.properties[prop].index) {
			this.indexes[model][prop] = descr.properties[prop].type.name;
		}
	}.bind(this));
};
SSDB.prototype.save = function (model, data, callback) {
	var client = this.client;
	var indexs = this.indexes[model];
	data = this.forDb(model, data);
	client.hset(model, data.id, JSON.stringify(data), function (err) {
		if (err) return callback(err);
		var funcs1 = [], funcs2 = [];
		Object.keys(indexs).forEach(function (prop) {
			if (data.hasOwnProperty(prop)) {
				var index_key = 'i:' + model + ':' + prop;
				if (indexs[prop] == 'Number' || indexs[prop] == 'Date') {
					funcs1.push(function (cb) { client.zdel(index_key, data.id, cb); });
					funcs2.push(function (cb) { client.zset(index_key, data.id, Number(data[prop]), cb); });
				} else {
					funcs1.push(function (cb) { client.zdel(index_key + ':' + data[prop], data.id, cb); });
					funcs2.push(function (cb) { client.zset(index_key + ':' + data[prop], data.id, new Date().getTime(), cb); });
				}
			}
		});
		async.parallel(funcs1, function (err) {
			if (err) return callback(err);
			async.parallel(funcs2, callback);
		});
	});
};
SSDB.prototype.create = function (model, data, callback) {
	if (!data.id) data.id = this.id();
	this.save(model, data, function (err) {
		if (err) return callback(err, data.id);
		this.client.zset('i:' + model + ':id', data.id, new Date().getTime(), function (err) {
			callback(err, data.id);
		});
	}.bind(this));
};
SSDB.prototype.updateOrCreate = function (model, data, callback) {
	this.create(model, data, function (err, id) {
		data.id || data.id = id;
		callback(err, data);
	});
};
SSDB.prototype.updateAttributes = function (model, id, data, callback) {
	data.id = id;
	this.save(model, data, callback);
};
SSDB.prototype.exists = function (model, id, callback) {
	this.client.hexists(model, id, callback);
};
SSDB.prototype.find = function (model, id, callback) {
	this.client.hget(model, id, function (err, data) {
		if (data) {
			callback(err, this.fromDb(model, JSON.parse(data)));
		} else {
			callback(err, null);
		}
	}.bind(this));
};
SSDB.prototype.ids = function (model, filter, callback) {
	var client = this.client;
	var indexs = this.indexes[model];
	var limit = filter.limit || 20;
	var order_index_key = 'i:' + model + ':id', order_adesc = 'desc';
	if (filter.order) {
		var m = filter.order.match(/\s+(asc|desc)$/i);
		if (m) {
			var prop = filter.order.replace(/\s+(asc|desc)/i, '');
			if (indexs[prop] && (indexs[prop] == 'Number' || indexs[prop] == 'Date')) {
				order_index_key = 'i:' + model + ':' + prop;
				if (m[1].toLowerCase() == 'asc') order_adesc = 'asc';
			}
		}
	}
	if (filter.where) {
		var matched_indexes = [];
		Object.keys(filter.where).forEach(function (prop) {
			if (indexs[prop]) {
				var index_key = 'i:' + model + ':' + prop;
				switch (indexs[prop]) {
					case 'Boolean':
						matched_indexes.push(index_key + ':' + (filter.where[prop] ? '1' : '0'));
						break;
					case 'String':
					case 'Text':
						if (typeof filter.where[prop] === 'string') {
							matched_indexes.push(index_key + ':' + filter.where[prop]);
						}
				}
			}
		});
		if (matched_indexes.length) {
			async.map(matched_indexes, function(index_key, cb) {
				client.zscan(index_key, '', '', '', 1000, cb);
			}, function (err, results) {
				var ids = _.intersection(results);
				callback(err, ids.slice(0, limit));
			});
		} else {
			callback(null, []);
		}
	} else {
		if (order_adesc == 'desc') {
			client.zrscan(order_index_key, '', '', '', limit, callback);
		} else {
			client.zscan(order_index_key, '', '', '', limit, callback);
		}
	}
};
SSDB.prototype.all = function (model, filter, callback) {
	var client = this.client;
	var ssdb = this;
	this.ids(model, filter, function (err, ids) {
		client.multi_hget(model, ids, function (err, results) {
			if (err) return callback(err);
			var datas = (results || []).map(function (data) {
				return ssdb.fromDb(model, JSON.parse(data));
			});
			callback(err, datas);
		});
	});
};
SSDB.prototype.count = function (model, callback, where) {
	if (where) {
		var client = this.client;
		var indexs = this.indexes[model];
		var matched_indexes = [];
		Object.keys(where).forEach(function (prop) {
			if (indexs[prop]) {
				var index_key = 'i:' + model + ':' + prop;
				switch (indexs[prop]) {
					case 'Boolean':
						matched_indexes.push(index_key + ':' + (where[prop] ? '1' : '0'));
						break;
					case 'String':
					case 'Text':
						if (typeof where[prop] === 'string') {
							matched_indexes.push(index_key + ':' + where[prop]);
						}
				}
			}
		});
		if (matched_indexes.length) {
			async.map(matched_indexes, function(index_key, cb) {
				client.zscan(index_key, '', '', '', 1000, cb);
			}, function (err, results) {
				var ids = _.intersection(results);
				callback(err, ids.length);
			});
		} else {
			callback(null, 0);
		}
	} else {
		this.client.hsize(model, callback);
	}
};
SSDB.prototype.destroy = function (model, id, callback) {
	var client = this.client;
	var indexs = this.indexes[model];
	client.hget(model, id, function (err, data) {
		if (err || !data) return callback(err);
		data = JSON.parse(data);
		var funcs = [];
		funcs.push(function (cb) { client.hdel(model, id, cb); });
		funcs.push(function (cb) { client.zdel('i:' + model + ':id', id, cb); });
		Object.keys(indexs).forEach(function (prop) {
			if (data.hasOwnProperty(prop)) {
				var index_key = 'i:' + model + ':' + prop;
				if (indexs[prop] == 'Number' || indexs[prop] == 'Date') {
					funcs.push(function (cb) { client.zdel(index_key, id, cb); });
				} else {
					funcs.push(function (cb) { client.zdel(index_key + ':' + data[prop], id, cb); });
				}
			}
		});
		async.parallel(funcs, callback);
	});
};
SSDB.prototype.destroyAll = function (model, callback) {
	var client = this.client;
	async.parallel([
		function (cb) { client.hclear(model, cb); },
		function (cb) {
			var funcs = [];
			client.zlist('i:' + model + ':', '', 100, function (err, index_keys) {
				index_keys.forEach(function (index_key) {
					if (index_key.indexOf('i:' + model + ':') == 0) {
						funcs.push(function (cb) { client.zclear(index_key, cb); });
					}
				});
			});
			async.parallel(funcs, cb);
		}
	], callback);
};
SSDB.prototype.disconnect = function (callback) {
	this.client.quit(callback);
};
