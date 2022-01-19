local pb = require "pb"
local grpc = require "kong.tools.grpc"

local exiting = ngx.worker.exiting

local encode = pb.encode
local decode = pb.decode
local send = nil
local receive = nil

local wrpc = {}

local _service_cache = {}
local _all_services = {}

local pp = require "pl.pretty".debug

local function proto_searchpath(name)
  return package.searchpath(name, "/usr/include/?.proto;../koko/internal/wrpc/proto/?.proto;../go-wrpc/wrpc/internal/wrpc/?.proto")
end

--- injects dependencies
function wrpc.inject(opts)
  encode = opts.encode or encode
  decode = opts.decode or decode
  send = opts.send or send
  receive = opts.receive or receive
end

local wrpc_proto

--- loads a service from a .proto file
--- returns a table relating the wRPC ids with
--- the scoped names and types
function wrpc.load_service(service_name)
  do
    local service = _service_cache[service_name]
    if service ~= nil then
      return service
    end
  end

  if not wrpc_proto then
    local wrpc_protofname = assert(proto_searchpath("wrpc"))
    wrpc_proto = assert(grpc.each_method(wrpc_protofname))
    --pp("wrpc_proto", wrpc_proto)
  end

  local annotations = {
    service = {},
    rpc = {},
  }
  local service_fname = assert(proto_searchpath(service_name))
  local proto_f = assert(io.open(service_fname))
  local scope_name = nil

  for line in proto_f:lines() do
    local annotation = line:match("//%s*%+wrpc:%s*(.-)%s*$")
    if annotation then
      local nextline = proto_f:read("*l")
      local keyword, identifier = nextline:match("^%s*(%a+)%s+(%w+)")
      if keyword and identifier then

        if keyword == "service" then
          scope_name = identifier;

        elseif keyword == "rpc" then
          identifier = scope_name .. "." .. identifier
        end

        local type_annotations = annotations[keyword]
        if type_annotations then
          local tag_key, tag_value = annotation:match("^%s*(%S-)=(%S+)%s*$")
          if tag_key and tag_value then
            tag_value = tag_value
            local tags = type_annotations[identifier] or {}
            type_annotations[identifier] = tags
            tags[tag_key] = tag_value
          end
        end
      end
    end
  end
  proto_f:close()

  local service = {}
  grpc.each_method(service_fname, function(parsed, srvc, mthd)
    assert(srvc.name)
    assert(mthd.name)
    local rpc_name = srvc.name .. "." .. mthd.name

    local service_id = assert(annotations.service[srvc.name] and annotations.service[srvc.name]["service-id"])
    local rpc_id = assert(annotations.rpc[rpc_name] and annotations.rpc[rpc_name]["rpc-id"])
    local rpc = {
      name = rpc_name,
      service_id = tonumber(service_id),
      rpc_id = tonumber(rpc_id),
      input_type = mthd.input_type,
      output_type = mthd.output_type,
    }
    service[service_id .. ":" .. rpc_id] = rpc
    service[rpc_name] = rpc
    _all_services[service_id .. ":" .. rpc_id] = rpc
    _all_services[rpc_name] = rpc
  end, true)

  _service_cache[service_name] = service
  return service
end

local seq = 0
function wrpc.call(name, data)
  seq = seq + 1

  local rpc = _all_services[name]

  local msg = encode("wrpc.WebsocketPayload", {
    version = 1,
    payload = {
      mtype = 2, -- MESSAGE_TYPE_RPC,
      svc_id = rpc.service_id,
      rpc_id = rpc.rpc_id,
      seq = seq,
      deadline = ngx.now() + 10,
      payload_encoding = 1, -- ENCODING_PROTO3
      payloads = { encode(rpc.input_type, data), }
    },
  })
  send(msg)
  return seq
end


local function decode_payload(payload)
  local id = (payload.svc_id or "") .. ":" .. (payload.rpc_id or "")
  local rpc = _all_services[id]
  if not rpc then
    return nil, "INVALID_SERVICE"
  end

  local out = {
    seq = payload.seq,
    ack = payload.ack,
    deadline = payload.deadline,
    method_name = rpc.name,
    data = decode(rpc.output_type, payload.payloads),
  }

  return out
end

local _response_queue = {}

function wrpc.step()
  local msg = receive()

  while msg ~= nil do
    msg = assert(decode("wrpc.WebsocketPayload", msg))
    assert(msg.version == 1, "unknown encoding version")
    local payload = msg.payload
    if payload.mtype == 2 then
      -- MESSAGE_TYPE_RPC
      -- handle "protocol" stuff (deadline, encoding, versions, etc
      _response_queue[payload.ack] = assert(decode_payload(payload)).data

    end

    msg = receive()
  end
end

function wrpc.receive_thread()
  return ngx.thread.spawn(function()
    while not exiting() do
      wrpc.step()
    end
  end)
end

function wrpc.get_response(req_id)
  wrpc.step()

  local resp_data = _response_queue[req_id]
  _response_queue[req_id] = nil

  if resp_data == nil then
    return nil, "no response"
  end

  return resp_data
end


return wrpc
