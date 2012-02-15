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
  
  t1 = Benchmark.realtime {
    threads.each(&:join)
  }

  assert_equal "4950", redis.get("total_amount")

  puts "Executed in %.7f seconds" % t1
end
