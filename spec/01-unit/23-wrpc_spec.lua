local wrpc = require "kong.tools.wrpc"

local _mock_buffs = {}
local function clear_mock_buffers()
  _mock_buffs.send = {head = 0, tail = 0}
  _mock_buffs.receive = {head = 0, tail = 0}
  _mock_buffs.encode = {head = 0, tail = 0}
  _mock_buffs.decode = {head = 0, tail = 0}
end

local function mock_buffer_add(bufname, d)
  local buf = assert(_mock_buffs[bufname])
  buf[buf.head] = d
  buf.head = buf.head + 1
end

local function mock_buff_pop(bufname)
  local buf = assert(_mock_buffs[bufname])
  if buf.head > buf.tail then
    local d = buf[buf.tail]
    buf.tail = buf.tail + 1
    return d
  end

  return nil, "empty"
end

local function mock_encode(type, d)
  mock_buffer_add("encode", {type=type, data=d})
  return d
end

local function mock_decode(type, d)
  mock_buffer_add("decode", {type=type, data=d})
  return d
end

local function mock_send(d)
  return mock_buffer_add("send", d)
end

local function mock_receive()
  return mock_buff_pop("receive")
end


describe("wRPC tools", function()
  it("loads service definition", function()
    local srv = wrpc.load_service("kong.services.config.v1.config")

    local ping_method = srv["ConfigService.PingCP"]
    assert.is_table(ping_method)
    assert.same("ConfigService.PingCP", ping_method.name)
    assert.is_string(ping_method.input_type)
    assert.is_string(ping_method.output_type)
    local method_ref = ping_method.service_id .. ":" .. ping_method.rpc_id
    assert.equals(srv[method_ref], ping_method)
  end)

  it("rpc call", function()
    wrpc.inject{
      encode = mock_encode,
      decode = mock_decode,
      send = mock_send,
      receive = mock_receive,
    }

    wrpc.load_service("kong.services.config.v1.config")

    clear_mock_buffers()
    local req_data = {
      version = 2,
      config = {
        format_version = "0.1a",
      }
    }
    local call_id = assert.not_nil(wrpc.call("ConfigService.SyncConfig", req_data))

    assert.same({type = ".kong.services.config.v1.SyncConfigRequest", data = req_data}, mock_buff_pop("encode"))
    assert.same({type = "wrpc.WebsocketPayload", data = {
      version = 1,
      payload = {
        svc_id = 1,
        rpc_id = 2,
        seq = 1,
        mtype = 2,
        deadline = ngx.now() + 10,
        payload_encoding = 1,
        payloads = { req_data },
      },
    }}, mock_buff_pop("encode"))
    assert.same({
      version = 1,
      payload = {
        svc_id = 1,
        rpc_id = 2,
        seq = 1,
        mtype = 2,
        deadline = ngx.now() + 10,
        payload_encoding = 1,
        payloads = { req_data },
      },
    }, mock_buff_pop("send"))

    local response, err = wrpc.get_response(call_id)
    assert.is_nil(response)
    assert.equal("no response", err)

    mock_buffer_add("receive", {
      version = 1,
      payload = {
        mtype = 2,  -- MESSAGE_TYPE_RPC
        svc_id = 1, -- ConfigService
        rpc_id = 2, -- SyncConfig
        seq = 67,
        ack = call_id,
        deadline = math.huge,
        payload_encoding = 1, -- ENCODING_PROTO3
        payloads = { accepted = true }
      }
    })
    response, err = wrpc.get_response(call_id)
    assert.is_nil(err)
    assert.same({ accepted = true }, response)

  end)
end)
