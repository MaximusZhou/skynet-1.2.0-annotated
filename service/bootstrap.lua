-- 该脚本在lualib/loader.lua中最后一行代码处加载执行
--
local skynet = require "skynet" -- 即对应模块lualib/skynet.lua
local harbor = require "skynet.harbor" -- 即对应模块 lualib/skynet/harbor.lua
require "skynet.manager"	-- import skynet.launch, ...-- 即对应模块 lualib/skynet/manager.lua

 -- 对应模块为lualib-src/lua-memory.c，调用接口luaopen_skynet_memory
local memory = require "skynet.memory"

skynet.start(function()
	local sharestring = tonumber(skynet.getenv "sharestring" or 4096)
	memory.ssexpand(sharestring)

	local standalone = skynet.getenv "standalone"

	-- 启动一个新的snlua服务，加载对应的脚本service/launcher.lua
	local launcher = assert(skynet.launch("snlua","launcher"))
	-- 给启动的服务设置名字为.launcher
	skynet.name(".launcher", launcher)

	local harbor_id = tonumber(skynet.getenv "harbor" or 0)
	if harbor_id == 0 then
		assert(standalone ==  nil)
		standalone = true
		skynet.setenv("standalone", "true")

		local ok, slave = pcall(skynet.newservice, "cdummy")
		if not ok then
			skynet.abort()
		end
		skynet.name(".cslave", slave)

	else
		if standalone then
			if not pcall(skynet.newservice,"cmaster") then
				skynet.abort()
			end
		end

		local ok, slave = pcall(skynet.newservice, "cslave")
		if not ok then
			skynet.abort()
		end
		skynet.name(".cslave", slave)
	end

	if standalone then
		local datacenter = skynet.newservice "datacenterd"
		skynet.name("DATACENTER", datacenter)
	end
	skynet.newservice "service_mgr"
	pcall(skynet.newservice,skynet.getenv "start" or "main")
	-- lua bootstrap 服务退出
	skynet.exit()
end)
