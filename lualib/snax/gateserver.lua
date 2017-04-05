local skynet = require "skynet"
local netpack = require "netpack"
local socketdriver = require "socketdriver"

local gateserver = {}

local socket	-- listen socket
local queue		-- message queue
local maxclient	-- max client
local client_number = 0
local CMD = setmetatable({}, { __gc = function() netpack.clear(queue) end })
local nodelay = false

local connection = {}


-- 当接受一个客户连接时，需要显示的调用此函数，socketdriver.start会将此fd加入监听逻辑中
function gateserver.openclient(fd)
	if connection[fd] then
		socketdriver.start(fd)
	end
end
-- 主动关闭一个客户连接
function gateserver.closeclient(fd)
	local c = connection[fd]
	if c then
		connection[fd] = false
		socketdriver.close(fd)
	end
end


function gateserver.start(handler)
	assert(handler.message)
	assert(handler.connect)

	-- gateserver服务的"open"命令方法实现，监听conf.address:conf.port 
	-- 如果提供了handler.open方法，则会被调用	
	function CMD.open( source, conf )
		assert(not socket)
		local address = conf.address or "0.0.0.0"
		local port = assert(conf.port)
		maxclient = conf.maxclient or 1024
		nodelay = conf.nodelay
		skynet.error(string.format("Listen on %s:%d", address, port))
		socket = socketdriver.listen(address, port)
		socketdriver.start(socket)
		if handler.open then
			return handler.open(source, conf)
		end
	end

	-- 关闭监听套接字	
	function CMD.close()
		assert(socket)
		socketdriver.close(socket)
	end

	local MSG = {}

	-- 当收到数据刚刚好是一个完整数据包时调用此函数，将数据包交由用户提供的handler.message处理
	local function dispatch_msg(fd, msg, sz)
		if connection[fd] then
			handler.message(fd, msg, sz)
		else
			skynet.error(string.format("Drop message from fd (%d) : %s", fd, netpack.tostring(msg,sz)))
		end
	end

	MSG.data = dispatch_msg

	-- 当收到的数据大于一个完整包长度时调用此方法
	local function dispatch_queue()
		local fd, msg, sz = netpack.pop(queue)
		if fd then
			-- may dispatch even the handler.message blocked
			-- If the handler.message never block, the queue should be empty, so only fork once and then exit.
			skynet.fork(dispatch_queue)
			dispatch_msg(fd, msg, sz)

			for fd, msg, sz in netpack.pop, queue do
				dispatch_msg(fd, msg, sz)
			end
		end
	end

	MSG.more = dispatch_queue

	-- 接受一个新连接时的回调函数，函数中会调用用户提供handler.connect函数，参数为fd和ip地址
	function MSG.open(fd, msg)
		if client_number >= maxclient then
			socketdriver.close(fd)
			return
		end
		if nodelay then
			socketdriver.nodelay(fd)
		end
		connection[fd] = true
		client_number = client_number + 1
		handler.connect(fd, msg)
	end

	local function close_fd(fd)
		local c = connection[fd]
		if c ~= nil then
			connection[fd] = nil
			client_number = client_number - 1
		end
	end

	--客户关闭连接时的回调函数，	函数中会调用用户提供handler.disconnect函数
	function MSG.close(fd)
		if fd ~= socket then
			if handler.disconnect then
				handler.disconnect(fd)
			end
			close_fd(fd)
		else
			socket = nil
		end
	end

	function MSG.error(fd, msg)
		if fd == socket then
			socketdriver.close(fd)
			skynet.error(msg)
		else
			if handler.error then
				handler.error(fd, msg)
			end
			close_fd(fd)
		end
	end

	function MSG.warning(fd, size)
		if handler.warning then
			handler.warning(fd, size)
		end
	end

	-- 注册skynet.PTYPE_SOCKET套接字类型的消息处理方法，skynet在收到套接字的相关消息时，会调用这里定义的
	-- dispatch函数，然后根据消息类型type调用消息处理方法MSG[type]
	skynet.register_protocol {
		name = "socket",
		id = skynet.PTYPE_SOCKET,	-- PTYPE_SOCKET = 6
		unpack = function ( msg, sz )
			return netpack.filter( queue, msg, sz)
		end,
		dispatch = function (_, _, q, type, ...)
			queue = q
			if type then
				MSG[type](...)
			end
		end
	}

	-- 此服务的lua协议处理方法，此服务只提供了open和close命令，分别用于启用监听套接字和关闭监听套接字
	-- 其他命令可交由用户提供的handler.command进行处理
	skynet.start(function()
		skynet.dispatch("lua", function (_, address, cmd, ...)
			local f = CMD[cmd]
			if f then
				skynet.ret(skynet.pack(f(address, ...)))
			else
				skynet.ret(skynet.pack(handler.command(cmd, address, ...)))
			end
		end)
	end)
end

return gateserver

--[[
使用方法：参见gate.lua里的使用
此服务提供最基本的套接字监听，消息组装成长度+消息体的组包服务，将一个个消息包进行分发处理
--]]
