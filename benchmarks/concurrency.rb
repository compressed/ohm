require "benchmark"
require "ohm"

Ohm.connect(:port => 6379, :db => 15)

def assert(condition, message)
  raise message if not condition
end

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

def calc_answer(n)
  ret = 0
  n.times { |i| ret += i }

  return ret
end

N = (ENV["ITERATIONS"] || 100).to_i
ANSWER = calc_answer(N)

Benchmark.bmbm do |x|
  x.report "CAS" do
    Ohm.flush

    threads = N.times.map { |i|
      Thread.new(add(i)) { |txn|
        txn.commit(redis)
      }
    }

    threads.each(&:join)

    assert(ANSWER == redis.get("total_amount").to_i, "CAS FAILED")
  end

  LUA = <<-LUA
  local total_amount = KEYS[1]
  local amount = ARGV[1]
  local current = redis.call("GET", total_amount)

  return redis.call("SET", total_amount, tonumber(current) + tonumber(amount))
  LUA

  require "digest/sha1"
  sha = Digest::SHA1.hexdigest(LUA)

  x.report "LUA" do
    Ohm.flush

    # Our script is a bit naive as it assumes the key
    # is already set, so we set it here.
    redis.set("total_amount", 0)

    # Let's dry run our lua script so we can evalsha from now on.
    redis.eval(LUA, 1, "total_amount", 0)

    threads = N.times.map { |i|
      Thread.new { redis.evalsha(sha, 1, "total_amount", i) }
    }

    threads.each(&:join)

    assert(ANSWER == redis.get("total_amount").to_i, "LUA FAILED")
  end

  require File.expand_path("./locking", File.dirname(__FILE__))

  module Foo
    extend Ohm::Locking

    def self.key
      @key ||= Nest.new(:Foo, Ohm.redis)
    end

    def self.incrby(n)
      mutex(0.001) do
        val = key.get.to_i

        key.set(val + n)
      end
    end
  end

  x.report "LOCKING" do
    Ohm.flush

    threads = N.times.map { |i|
      Thread.new(i) { |n| Foo.incrby(n) }
    }

    threads.each(&:join)

    assert(ANSWER == Foo.key.get.to_i, "LOCKING FAILED")
  end
end
