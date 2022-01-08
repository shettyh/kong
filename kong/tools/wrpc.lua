local protoc = require "protoc"
local pb = require "pb"
local grpc = require "kong.tools.grpc"

local pp = require "pl.pretty"

local wrpc = {}

local _service_cache = {}

local function proto_searchpath(name)
  return package.searchpath(name, "/usr/include/?.proto;/home/javier/devel/kong_dev/koko/internal/wrpc/proto/?.proto")
end

local function protobuf_parse(fname)
  local p = protoc.new()
  p:addpath("/usr/include")
  p:addpath("/home/javier/devel/kong_dev/koko/internal/wrpc/proto")
  p.include_imports = true
  p:loadfile(fname)
  return p:parsefile(fname)
end


local function merge(dst, src)
  for k, v in pairs(src) do
    dst[k] = v
  end
  return dst
end

local function add_meta(target, datas)
  target.wrpc = merge(target.wrpc or {}, datas[target.name] or {})
end


--- loads a service from a .proto file
--- including wrpc annotations
function wrpc.load_service(service_name)
  local service = _service_cache[service_name]
  if service ~= nil then
    return service
  end

  local service_fname = assert(proto_searchpath(service_name))
  local proto_f = assert(io.open(service_fname))
  local annotations = {
    service = {},
    rpc = {},
  }
  for line in proto_f:lines() do -- io.lines(service_fname) do
    local annotation = line:match("//%s*%+wrpc:%s*(.-)%s*$")
    if annotation then
      local nextline = proto_f:read("*l")
      local keyword, identifier = nextline:match("^%s*(%a+)%s+(%w+)")
      if keyword and identifier then
        local type_annotations = annotations[keyword]
        if type_annotations then
          local tag_key, tag_value = annotation:match("^%s*(%S-)=(%S+)%s*$")
          if tag_key and tag_value then
            tag_value = tonumber(tag_value) or tag_value
            local tags = type_annotations[identifier] or {}
            type_annotations[identifier] = tags
            tags[tag_key] = tag_value
          end
        end
      end
    end
  end
  proto_f:close()

  service = grpc.each_method(service_fname, function(parsed, srvc, mthd)
    add_meta(srvc, annotations.service)
    add_meta(mthd, annotations.rpc)
  end, true)

  _service_cache[service_name] = service

  return service
end


return wrpc
