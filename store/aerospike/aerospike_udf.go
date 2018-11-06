package aerospike

const listUDF = `
local function key_prefix_check(prefix)
    return function(rec)
        local key = record.key(rec)
        if key and type(key) == "string" then
            if string.find(key, prefix) == 1 then
                return true
            else
                return false
            end
        else
            return false
        end
    end
end

local function map_record(rec)
  return map { key=record.key(rec), bin=rec['bin'], gen=record.gen(rec) }
end

function list(stream, prefix)
  return stream : filter(key_prefix_check(prefix)) : map(map_record)
end
`
