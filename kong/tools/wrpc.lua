local pb = require "pb"
local grpc = require "kong.tools.grpc"

local exiting = ngx.worker.exiting

local wrpc = {}

local pp = require "pl.pretty".debug


local function merge(a, b)
  if type(b) == "table" then
    for k, v in pairs(b) do
      a[k] = v
    end
  end

  return a
end

local function proto_searchpath(name)
  return package.searchpath(name, "/usr/include/?.proto;../koko/internal/wrpc/proto/?.proto;../go-wrpc/wrpc/internal/wrpc/?.proto")
end

local wrpc_proto

local wrpc_service = {}
wrpc_service.__index = wrpc_service



function wrpc.new_service()
  if not wrpc_proto then
    local wrpc_protofname = assert(proto_searchpath("wrpc"))
    wrpc_proto = assert(grpc.each_method(wrpc_protofname))
    --pp("wrpc_proto", wrpc_proto)
  end

  return setmetatable({
    methods = {},
  }, wrpc_service)
end


function wrpc_service:add(service_name)
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
    self.methods[service_id .. ":" .. rpc_id] = rpc
    self.methods[rpc_name] = rpc
  end, true)
end


function wrpc_service:get_method(srvc_id, rpc_id)
  local rpc_name
  if type(srvc_id) == "string" and rpc_id == nil then
    rpc_name = srvc_id
  else
    rpc_name = tostring(srvc_id) .. ":" .. tostring(rpc_id)
  end

  return self.methods[rpc_name]
end


local wrpc_peer = {
  encode = pb.encode,
  decode = pb.decode,
}
wrpc_peer.__index = wrpc_peer

function wrpc.new_peer(conn, service, opts)
  return setmetatable(merge({
    conn = conn,
    service = service,
    seq = 0,
    response_queue = {},
  }, opts), wrpc_peer)
end


function wrpc_peer:send(d)
  self.conn:send_binary(d)
end

function wrpc_peer:receive()
  while true do
    local data, typ, err = self.conn:recv_frame()
    if not data then
      return nil, err
    end

    if typ == "binary" then
      return data
    end
  end
end

function wrpc_peer:call(name, data)
  local seq = self.seq + 1

  local rpc = self.service:get_method(name)
  if not rpc then
    return nil, string.format("no method %q", name)
  end

  local msg = self.encode("wrpc.WebsocketPayload", {
    version = 1,
    payload = {
      mtype = 2, -- MESSAGE_TYPE_RPC,
      svc_id = rpc.service_id,
      rpc_id = rpc.rpc_id,
      seq = seq,
      deadline = ngx.now() + 10,
      payload_encoding = 1, -- ENCODING_PROTO3
      payloads = { self.encode(rpc.input_type, data), }
    },
  })
  self:send(msg)
  self.seq = seq
  return seq
end


local function decode_payload(self, payload)
  local rpc = self.service:get_method(payload.svc_id, payload.rpc_id)
  if not rpc then
    return nil, "INVALID_SERVICE"
  end

  local out = {
    seq = payload.seq,
    ack = payload.ack,
    deadline = payload.deadline,
    method_name = rpc.name,
    data = self.decode(rpc.output_type, payload.payloads),
  }

  return out
end


function wrpc_peer:step()
  local msg = self:receive()

  while msg ~= nil do
    msg = assert(self.decode("wrpc.WebsocketPayload", msg))
    assert(msg.version == 1, "unknown encoding version")
    local payload = msg.payload
    if payload.mtype == 2 then
      -- MESSAGE_TYPE_RPC
      -- handle "protocol" stuff (deadline, encoding, versions, etc
      self.response_queue[payload.ack] = assert(decode_payload(self, payload)).data

    end

    msg = self:receive()
  end
end

function wrpc_peer:receive_thread()
  return ngx.thread.spawn(function()
    while not exiting() do
      self:step()
    end
  end)
end



function wrpc_peer:get_response(req_id)
  self:step()

  local resp_data = self.response_queue[req_id]
  self.response_queue[req_id] = nil

  if resp_data == nil then
    return nil, "no response"
  end

  return resp_data
end


return wrpc
