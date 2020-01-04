-- 这个脚本的全局变量都是在 init_cb (service-src/service_snlua) 中设置的
local args = {}
for word in string.gmatch(..., "%S+") do
	table.insert(args, word)
end

-- 按config/examples 中配置值就是 bootstrap
SERVICE_NAME = args[1]

local main, pattern

local err = {}
for pat in string.gmatch(LUA_SERVICE, "([^;]+);*") do
	local filename = string.gsub(pat, "?", SERVICE_NAME)
	local f, msg = loadfile(filename) -- 按 config/examples 加载的就是 serivce/bootstrap.lua
	if not f then
		table.insert(err, msg)
	else
		pattern = pat
		main = f
		break
	end
end

if not main then
	error(table.concat(err, "\n"))
end

-- 下面都是准备环境，为执行serivce/bootstrap.lua做准备
LUA_SERVICE = nil
-- 把 LUA_PATH 的值赋值给package.path，同时赋值 LUA_PATH 为nil
package.path , LUA_PATH = LUA_PATH
package.cpath , LUA_CPATH = LUA_CPATH

local service_path = string.match(pattern, "(.*/)[^/?]+$")

if service_path then
	service_path = string.gsub(service_path, "?", args[1])
	package.path = service_path .. "?.lua;" .. package.path
	SERVICE_PATH = service_path
else
	local p = string.match(pattern, "(.*/).+$")
	SERVICE_PATH = p
end

-- 执行 examples/config 中的的 preload 配置
if LUA_PRELOAD then
	local f = assert(loadfile(LUA_PRELOAD))
	f(table.unpack(args))
	LUA_PRELOAD = nil
end

-- 执行service/bootstrap.lua
main(select(2, table.unpack(args)))
