local wrpc = require "kong.tools.wrpc"

local pp = require "pl.pretty"

describe("wRPC tools", function()
  it("loads service definition", function()
    local srv = wrpc.load_service("kong.services.config.v1.config")
    pp(srv)
  end)
end)
