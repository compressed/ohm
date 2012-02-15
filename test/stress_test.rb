# encoding: UTF-8

require File.expand_path("./helper", File.dirname(__FILE__))
require "benchmark"

def redis
  Ohm.redis
end

def add(n)
  Ohm::Transaction.define { |t|
    t.watch("total_amount")

    t.read do |store|
      store.amount = redis.get("total_amount")
    end

    t.write do |store|
      redis.set("total_amount", store.amount.to_i + n)
    end
  }
end

test do
  threads = 100.times.map { |i|
    Thread.new(add(i)) { |txn|
      txn.commit(redis)
    }
  }

  t1 = Benchmark.realtime { threads.each(&:join) }

  assert_equal "4950", redis.get("total_amount")

  puts "Executed in %.7f seconds" % t1
end

LUA = <<-LUA
local total_amount = KEYS[1]
local amount = ARGV[1]
local current = redis.call("GET", total_amount)

return redis.call("SET", total_amount, tonumber(current) + tonumber(amount))
LUA

test do
  # Our script is a bit naive as it assumes the key
  # is already set, so we set it here.
  redis.set("total_amount", 0)

  # Let's dry run our lua script so we can evalsha from now on.
  redis.eval(LUA, 1, "total_amount", 0)

  # Now we can use the sha for running our script.
  require "digest/sha1"
  sha = Digest::SHA1.hexdigest(LUA)

  threads = 100.times.map { |i|
    Thread.new { redis.evalsha(sha, 1, "total_amount", i) }
  }

  t1 = Benchmark.realtime { threads.each(&:join) }

  assert_equal "4950", redis.get("total_amount")

  puts "Executed in %.7f seconds" % t1
end
