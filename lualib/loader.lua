-- 这个脚本是在初始化snlua服务的时候，在接口init_cb (service-src/service_snlua)中调用的
-- 这个脚本的全局变量也都是在 init_cb (service-src/service_snlua) 中设置的
local args = {}
for word in string.gmatch(..., "%S+") do
	table.insert(args, word)
end

-- 按config/examples 中配置, SERVICE_NAME值就是 bootstrap
-- 在脚本中使用skynet.launch("snlua","launcher")调用的时候，这个地方SERVICE_NAME就是launcher
SERVICE_NAME = args[1]

local main, pattern

-- LUA_SERVICE 在examples/config.path配置，默认值就是
-- ./service/?.lua;./test/?.lua;./examples/?.lua;./test/?/init.lua
-- 也就是说从目录 service/ test/ examples/ /tes/name/init.lua 找相应的lua服务对应的lua文件
-- 直到找到为止
local err = {}
for pat in string.gmatch(LUA_SERVICE, "([^;]+);*") do
	local filename = string.gsub(pat, "?", SERVICE_NAME)
	-- 按 config/examples 配置 对于bootstrap服务，这里的filename就是 比如serivce/bootstrap.lua
	local f, msg = loadfile(filename) 
	if not f then
		table.insert(err, msg)
	else
		pattern = pat
		main = f
		break
	end
end

-- 按 config/examples 配置，到这里 pattern 就是 service/?.lua，
-- main就是 比如serivce/bootstrap.lua loadfile返回的结果
if not main then
	error(table.concat(err, "\n"))
end

-- 下面都是准备环境，为执行比如serivce/bootstrap.lua做准备
LUA_SERVICE = nil
-- 把 LUA_PATH 的值赋值给package.path，同时赋值 LUA_PATH 为nil
-- 上面执行后，按examples/config.pat配置
-- package.path就是./lualib/?.lua;./lualib/?/init.lua
-- path.cpath 就是 /luaclib/?.so
package.path , LUA_PATH = LUA_PATH
package.cpath , LUA_CPATH = LUA_CPATH

local service_path = string.match(pattern, "(.*/)[^/?]+$")

if service_path then
	service_path = string.gsub(service_path, "?", args[1])
	package.path = service_path .. "?.lua;" .. package.path
	SERVICE_PATH = service_path
else
	-- 默认执行到这个分支，执行完成后，SERVICE_PATH的值为 serivce/
	local p = string.match(pattern, "(.*/).+$")
	SERVICE_PATH = p
end

-- 执行 examples/config 中的的 preload 配置
if LUA_PRELOAD then
	local f = assert(loadfile(LUA_PRELOAD))
	f(table.unpack(args))
	LUA_PRELOAD = nil
end

-- 比如执行service/bootstrap.lua
main(select(2, table.unpack(args)))
