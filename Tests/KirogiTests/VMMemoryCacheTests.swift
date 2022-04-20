//
//  VMMemoryCacheTests.swift
//  KirogiTests
//
//  Created by Max on 2022/3/22.
//

import XCTest
@testable import Kirogi

class VMMemoryCacheTests: XCTestCase {
  
  var memoryCache: VMMemoryCache<String, User>!
  var benchmarkMemoryCache: VMMemoryCache<String, Data>!
  
  override func setUp() {
    super.setUp()
    
    self.memoryCache = VMMemoryCache<String, User>.initialize()
    self.benchmarkMemoryCache = VMMemoryCache<String, Data>.initialize()
  }
  
  override func tearDown() {
    self.memoryCache.removeAllObjects()
    self.benchmarkMemoryCache .removeAllObjects()
    
    super.tearDown()
  }
  
  func test_property() {
    XCTAssertNil(self.memoryCache.name)
    self.memoryCache.name = "kirogi"
    XCTAssertEqual(self.memoryCache.name, "kirogi")
    
    self.memoryCache.setObject(User(name: "max", age: 1), forKey: "user_max", withCost: 90)
    XCTAssertEqual(self.memoryCache.totalCost, 90)
    
    self.memoryCache.setObject(User(name: "max", age: 1), forKey: "user_max")
    XCTAssertEqual(self.memoryCache.totalCount, 1)
    
    XCTAssertEqual(self.memoryCache.costLimit, .max)
    self.memoryCache.costLimit = 250
    XCTAssertEqual(self.memoryCache.costLimit, 250)
    
    XCTAssertEqual(self.memoryCache.countLimit, .max)
    self.memoryCache.countLimit = 99
    XCTAssertEqual(self.memoryCache.countLimit, 99)
    
    XCTAssertEqual(self.memoryCache.ageLimit, .greatestFiniteMagnitude)
    self.memoryCache.ageLimit = 60.0 * 60.0
    XCTAssertEqual(self.memoryCache.ageLimit, 60.0 * 60.0)
    
    XCTAssertEqual(self.memoryCache.autoTrimInterval, 5.0)
    self.memoryCache.ageLimit = 10.0
    XCTAssertEqual(self.memoryCache.ageLimit, 10.0)
    
    XCTAssertEqual(self.memoryCache.shouldRemoveAllOnMemoryWarning, true)
    self.memoryCache.shouldRemoveAllOnMemoryWarning = false
    XCTAssertEqual(self.memoryCache.shouldRemoveAllOnMemoryWarning, false)
    
    XCTAssertEqual(self.memoryCache.shouldRemoveAllWhenEnterBackground, true)
    self.memoryCache.shouldRemoveAllWhenEnterBackground = false
    XCTAssertEqual(self.memoryCache.shouldRemoveAllWhenEnterBackground, false)
    
    XCTAssertEqual(self.memoryCache.releaseOnMainThread, false)
    self.memoryCache.releaseOnMainThread = true
    XCTAssertEqual(self.memoryCache.releaseOnMainThread, true)
    
    XCTAssertEqual(self.memoryCache.releaseAsynchronously, true)
    self.memoryCache.releaseAsynchronously = false
    XCTAssertEqual(self.memoryCache.releaseAsynchronously, false)
  }
    
  func test_contains() {
    XCTAssertFalse(self.memoryCache.contains(forKey: nil))
    
    self.memoryCache.setObject(User(name: "max", age: 28), forKey: "user_max")
    XCTAssertTrue(self.memoryCache.contains(forKey: "user_max"))
    XCTAssertFalse(self.memoryCache.contains(forKey: "kirogi"))
  }
  
  func test_setObject() {
    self.memoryCache.setObject(User(name: "max", age: 28), forKey: nil)
    XCTAssertNil(self.memoryCache.object(forKey: "user_max"))
    
    self.memoryCache.setObject(User(name: "max", age: 28), forKey: "user_max", withCost: 219)
    self.memoryCache.setObject(nil, forKey: "user")
    XCTAssertNil(self.memoryCache.object(forKey: "user"))
    
    self.memoryCache.setObject(User(name: "max", age: 28), forKey: "user_max")
    XCTAssertEqual(self.memoryCache.totalCost, 0)
    XCTAssertEqual(self.memoryCache.totalCount, 1)
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_max"))
    XCTAssertEqual(self.memoryCache.object(forKey: "user_max")?.name, "max")
    XCTAssertEqual(self.memoryCache.object(forKey: "user_max")?.age, 28)
        
    self.memoryCache.setObject(User(name: "jian", age: 21), forKey: "user_jian", withCost: 101)
    XCTAssertEqual(self.memoryCache.totalCost, 101)
    XCTAssertEqual(self.memoryCache.totalCount, 2)
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_jian"))
    XCTAssertEqual(self.memoryCache.object(forKey: "user_jian")?.name, "jian")
    XCTAssertEqual(self.memoryCache.object(forKey: "user_jian")?.age, 21)
            
    self.memoryCache.costLimit = 100
    self.memoryCache.countLimit = .max
    self.memoryCache.setObject(User(name: "max", age: 28), forKey: "user_max")
    usleep(100)
    XCTAssertEqual(self.memoryCache.totalCost, 0)
    XCTAssertEqual(self.memoryCache.totalCount, 1)
    XCTAssertNil(self.memoryCache.object(forKey: "user_jian"))
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_max"))
    XCTAssertEqual(self.memoryCache.object(forKey: "user_max")?.name, "max")
    XCTAssertEqual(self.memoryCache.object(forKey: "user_max")?.age, 28)
        
    self.memoryCache.costLimit = .max
    self.memoryCache.countLimit = 1
    self.memoryCache.setObject(User(name: "jian", age: 1), forKey: "user_jian", withCost: 99)
    usleep(100)
    XCTAssertEqual(self.memoryCache.totalCost, 99)
    XCTAssertEqual(self.memoryCache.totalCount, 1)
    XCTAssertNil(self.memoryCache.object(forKey: "user_max"))
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_jian"))
    XCTAssertEqual(self.memoryCache.object(forKey: "user_jian")?.name, "jian")
    XCTAssertEqual(self.memoryCache.object(forKey: "user_jian")?.age, 1)
    
    self.memoryCache.costLimit = .max
    self.memoryCache.countLimit = 1
    self.memoryCache.setObject(User(name: "jian", age: 1), forKey: "user_jian", withCost: 99)
    usleep(100)
    XCTAssertEqual(self.memoryCache.totalCost, 99)
    XCTAssertEqual(self.memoryCache.totalCount, 1)
    XCTAssertNil(self.memoryCache.object(forKey: "user_max"))
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_jian"))
    XCTAssertEqual(self.memoryCache.object(forKey: "user_jian")?.name, "jian")
    XCTAssertEqual(self.memoryCache.object(forKey: "user_jian")?.age, 1)
    
    self.memoryCache.costLimit = .max
    self.memoryCache.countLimit = 1
    self.memoryCache.releaseOnMainThread = true
    self.memoryCache.setObject(User(name: "max", age: 16), forKey: "user_max", withCost: 99)
    usleep(100)
    XCTAssertEqual(self.memoryCache.totalCost, 99)
    XCTAssertEqual(self.memoryCache.totalCount, 1)
    XCTAssertNil(self.memoryCache.object(forKey: "user_jian"))
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_max"))
    XCTAssertEqual(self.memoryCache.object(forKey: "user_max")?.name, "max")
    XCTAssertEqual(self.memoryCache.object(forKey: "user_max")?.age, 16)
    
    let expectation = self.expectation(description: "test VMMemoryCache setObject(_:forKey:withCost:) method")
    
    self.memoryCache.costLimit = .max
    self.memoryCache.countLimit = 1
    self.memoryCache.releaseAsynchronously = false
    self.memoryCache.releaseOnMainThread = true
    DispatchQueue.global().async {
      self.memoryCache.setObject(User(name: "jian", age: 27), forKey: "user_jian")
      usleep(100)
      XCTAssertEqual(self.memoryCache.totalCost, 0)
      XCTAssertEqual(self.memoryCache.totalCount, 1)
      XCTAssertNil(self.memoryCache.object(forKey: "user_max"))
      XCTAssertNotNil(self.memoryCache.object(forKey: "user_jian"))
      XCTAssertEqual(self.memoryCache.object(forKey: "user_jian")?.name, "jian")
      XCTAssertEqual(self.memoryCache.object(forKey: "user_jian")?.age, 27)
      
      expectation.fulfill()
    }
    
    self.wait(for: [expectation], timeout: 1.0)
  }
  
  func test_object() {
    XCTAssertNil(self.memoryCache.object(forKey: nil))
    
    XCTAssertNil(self.memoryCache.object(forKey: "user"))
    
    self.memoryCache.setObject(User(name: "max", age: 26), forKey: "user_max")
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_max"))
    XCTAssertEqual(self.memoryCache.object(forKey: "user_max")?.name, "max")
    XCTAssertEqual(self.memoryCache.object(forKey: "user_max")?.age, 26)
    
    self.memoryCache.setObject(User(name: "max", age: 28), forKey: "user_max")
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_max"))
    XCTAssertEqual(self.memoryCache.object(forKey: "user_max")?.name, "max")
    XCTAssertEqual(self.memoryCache.object(forKey: "user_max")?.age, 28)
  }
  
  func test_removeObject() {
    self.memoryCache.setObject(User(name: "max", age: 26), forKey: "user_max")
    
    self.memoryCache.removeObject(forKey: nil)
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_max"))
    
    self.memoryCache.removeObject(forKey: "user_jian")
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_max"))
    
    self.memoryCache.setObject(User(name: "jian", age: 16), forKey: "user_jian", withCost: 99)
    self.memoryCache.removeObject(forKey: "user_max")
    XCTAssertNil(self.memoryCache.object(forKey: "user_max"))
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_jian"))
    
    self.memoryCache.releaseOnMainThread = true
    self.memoryCache.setObject(User(name: "max", age: 16), forKey: "user_max", withCost: 99)
    self.memoryCache.removeObject(forKey: "user_jian")
    XCTAssertNil(self.memoryCache.object(forKey: "user_jian"))
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_max"))
    
    let expectation = self.expectation(description: "test VMMemoryCache removeObject(forKey:) method")
    
    self.memoryCache.releaseAsynchronously = false
    self.memoryCache.releaseOnMainThread = true
    self.memoryCache.setObject(User(name: "jian", age: 27), forKey: "user_jian")
    DispatchQueue.global().async {
      self.memoryCache.removeObject(forKey: "user_max")
      XCTAssertNil(self.memoryCache.object(forKey: "user_max"))
      XCTAssertNotNil(self.memoryCache.object(forKey: "user_jian"))
      
      expectation.fulfill()
    }
    
    self.wait(for: [expectation], timeout: 1.0)
  }
  
  func test_removeAllObjects() {
    self.memoryCache.setObject(User(name: "max", age: 1), forKey: "user_max")
    self.memoryCache.setObject(User(name: "jian", age: 2), forKey: "user_jian", withCost: 99)
    
    XCTAssertEqual(self.memoryCache.totalCost, 99)
    XCTAssertEqual(self.memoryCache.totalCount, 2)
    
    self.memoryCache.removeAllObjects()
    XCTAssertEqual(self.memoryCache.totalCost, 0)
    XCTAssertEqual(self.memoryCache.totalCount, 0)
    XCTAssertNil(self.memoryCache.object(forKey: "user_max"))
    XCTAssertNil(self.memoryCache.object(forKey: "user_jian"))
  }
  
  func test_trimForCost() {
    self.memoryCache.setObject(User(name: "max", age: 1), forKey: "user_max", withCost: 201)
    self.memoryCache.setObject(User(name: "jian", age: 1), forKey: "user_jian", withCost: 109)
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_max"))
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_jian"))
    self.memoryCache.trim(forCost: 0)
    usleep(100)
    XCTAssertNil(self.memoryCache.object(forKey: "user_max"))
    XCTAssertNil(self.memoryCache.object(forKey: "user_jian"))
    
    self.memoryCache.setObject(User(name: "max", age: 1), forKey: "user_max", withCost: 201)
    self.memoryCache.setObject(User(name: "jian", age: 1), forKey: "user_jian", withCost: 209)
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_max"))
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_jian"))
    self.memoryCache.trim(forCost: 500)
    usleep(100)
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_max"))
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_jian"))
    
    self.memoryCache.trim(forCost: 210)
    usleep(100)
    XCTAssertNil(self.memoryCache.object(forKey: "user_max"))
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_jian"))
    
    self.memoryCache.releaseOnMainThread = true
    self.memoryCache.setObject(User(name: "jian", age: 1), forKey: "user_jian", withCost: 109)
    self.memoryCache.setObject(User(name: "max", age: 1), forKey: "user_max", withCost: 199)
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_jian"))
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_max"))
    self.memoryCache.trim(forCost: 200)
    usleep(100)
    XCTAssertNil(self.memoryCache.object(forKey: "user_jian"))
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_max"))
  }
  
  func test_trimForCount() {
    self.memoryCache.setObject(User(name: "max", age: 1), forKey: "user_max")
    self.memoryCache.setObject(User(name: "jian", age: 1), forKey: "user_jian")
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_max"))
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_jian"))
    self.memoryCache.trim(forCount: 0)
    usleep(100)
    XCTAssertNil(self.memoryCache.object(forKey: "user_max"))
    XCTAssertNil(self.memoryCache.object(forKey: "user_jian"))
    
    self.memoryCache.setObject(User(name: "max", age: 1), forKey: "user_max")
    self.memoryCache.setObject(User(name: "jian", age: 1), forKey: "user_jian")
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_max"))
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_jian"))
    self.memoryCache.trim(forCount: 3)
    usleep(100)
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_max"))
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_jian"))
    
    self.memoryCache.trim(forCount: 1)
    usleep(100)
    XCTAssertNil(self.memoryCache.object(forKey: "user_max"))
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_jian"))
    
    self.memoryCache.releaseOnMainThread = true
    self.memoryCache.setObject(User(name: "jian", age: 1), forKey: "user_jian")
    self.memoryCache.setObject(User(name: "max", age: 1), forKey: "user_max")
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_jian"))
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_max"))
    self.memoryCache.trim(forCount: 1)
    usleep(100)
    XCTAssertNil(self.memoryCache.object(forKey: "user_jian"))
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_max"))
  }
  
  func test_trimForAge() {
    self.memoryCache.setObject(User(name: "max", age: 1), forKey: "user_max")
    self.memoryCache.setObject(User(name: "jian", age: 1), forKey: "user_jian")
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_max"))
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_jian"))
    usleep(100)
    self.memoryCache.trim(forAge: 0.0)
    usleep(100)
    XCTAssertNil(self.memoryCache.object(forKey: "user_max"))
    XCTAssertNil(self.memoryCache.object(forKey: "user_jian"))
    
    self.memoryCache.setObject(User(name: "max", age: 1), forKey: "user_max")
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_max"))
    usleep(100_000)
    self.memoryCache.setObject(User(name: "jian", age: 1), forKey: "user_jian")
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_jian"))
    self.memoryCache.trim(forAge: 2.0)
    usleep(100_000)
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_max"))
    usleep(100_000)
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_jian"))
    
    self.memoryCache.trim(forAge: 0.09)
    usleep(100)
    XCTAssertNil(self.memoryCache.object(forKey: "user_max"))
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_jian"))
    
    self.memoryCache.releaseOnMainThread = true
    self.memoryCache.setObject(User(name: "jian", age: 1), forKey: "user_jian")
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_jian"))
    usleep(100_000)
    self.memoryCache.setObject(User(name: "max", age: 1), forKey: "user_max")
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_max"))
    self.memoryCache.trim(forAge: 0.09)
    usleep(100)
    XCTAssertNil(self.memoryCache.object(forKey: "user_jian"))
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_max"))
  }
  
  func test_notification() {
    self.memoryCache.setObject(User(name: "max", age: 26), forKey: "user_max")
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_max"))
    NotificationCenter.default.post(name: UIApplication.didReceiveMemoryWarningNotification, object: nil)
    usleep(100)
    XCTAssertNil(self.memoryCache.object(forKey: "user_max"))
    
    self.memoryCache.setObject(User(name: "max", age: 26), forKey: "user_max")
    XCTAssertNotNil(self.memoryCache.object(forKey: "user_max"))
    NotificationCenter.default.post(name: UIApplication.didEnterBackgroundNotification, object: nil)
    usleep(100)
    XCTAssertNil(self.memoryCache.object(forKey: "user_max"))
  }
  
  func test_thread_safe() {
    let expectationCount = 50_000
    let expectation = self.expectation(description: "test VMMemoryCache thread safe")
    
    DispatchQueue.concurrentPerform(iterations: expectationCount) { (index) in
      self.memoryCache.setObject(User(name: "max_\(index)", age: index), forKey: "user_\(index)_max")
      
      DispatchQueue.global().async {
        XCTAssertNotNil(self.memoryCache.object(forKey: "user_\(index)_max"))
        
        if index == expectationCount - 1 {
          expectation.fulfill()
        }
      }
    }
    
    self.wait(for: [expectation], timeout: 1.0)
  }
  
  func test_set_200_000_key_value_pairs() {
    var keys = [String]()
    var values = [Data]()
    (0 ..< 200_000).forEach {
      keys.append("\($0)")
      values.append(withUnsafeBytes(of: $0, { Data($0) }))
    }
    
    print("memory cache set 200_000 key-value pairs")

    /// measured
    ///
    /// average: 0.977
    /// relative standard deviation: 7.769%
    /// values: [1.196299, 0.945033, 0.964819, 0.964539, 0.981629, 0.941689, 0.989135, 0.933922, 0.926800, 0.927795]
    self.measure {
      (0 ..< 200_000).forEach {
        self.benchmarkMemoryCache.setObject(values[$0], forKey: keys[$0])
      }
    }
  }
  
  func test_get_200_000_key_value_pairs() {
    var keys = [String]()
    var values = [Data]()
    (0 ..< 200_000).forEach {
      keys.append("\($0)")
      values.append(withUnsafeBytes(of: $0, { Data($0) }))
    }
    
    print("memory cache get 200_000 key-value pairs")

    (0 ..< 200_000).forEach {
      self.benchmarkMemoryCache.setObject(values[$0], forKey: keys[$0])
    }
    
    /// measured
    ///
    /// average: 0.839
    /// relative standard deviation: 1.436%
    /// values: [0.849424, 0.848977, 0.847632, 0.843890, 0.854552, 0.845778, 0.826175, 0.822410, 0.826526, 0.822802]
    self.measure {
      (0 ..< 200_000).forEach {
        _ = self.benchmarkMemoryCache.object(forKey: keys[$0])
      }
    }
  }
  
  func test_get_200_000_key_value_pairs_randomly() {
    var keys = [String]()
    var values = [Data]()
    (0 ..< 200_000).forEach {
      keys.append("\($0)")
      values.append(withUnsafeBytes(of: $0, { Data($0) }))
    }
    
    print("memory cache get 200_000 key-value pairs randomly")

    (0 ..< 200_000).forEach {
      self.benchmarkMemoryCache.setObject(values[$0], forKey: keys[$0])
    }
    
    (0 ..< keys.count).reversed().forEach {
      keys.swapAt($0, Int(arc4random_uniform(UInt32($0))))
    }
    
    /// measured
    ///
    /// average: 0.854
    /// relative standard deviation: 7.877%
    /// values: [1.050528, 0.870379, 0.852415, 0.833096, 0.825649, 0.821870, 0.818105, 0.821015, 0.826766, 0.822501]
    self.measure {
      (0 ..< 200_000).forEach {
        _ = self.benchmarkMemoryCache.object(forKey: keys[$0])
      }
    }
  }
  
  func test_get_200_000_key_value_pairs_none_exist() {
    var keys = [String]()
    var values = [Data]()
    (0 ..< 200_000).forEach {
      keys.append("\($0)")
      values.append(withUnsafeBytes(of: $0, { Data($0) }))
    }
    
    print("memory cache get 200_000 key-value pairs randomly")

    (0 ..< 200_000).forEach {
      self.benchmarkMemoryCache.setObject(values[$0], forKey: keys[$0])
    }
    
    (0 ..< 200_000).forEach {
      keys.append("\($0 + 200_000)")
    }
    
    (0 ..< keys.count).reversed().forEach {
      keys.swapAt($0, Int(arc4random_uniform(UInt32($0))))
    }
    
    /// measured
    ///
    /// average: 0.625
    /// relative standard deviation: 3.742%
    /// values: [0.689589, 0.636581, 0.617277, 0.632198, 0.618827, 0.615446, 0.610494, 0.609353, 0.611772, 0.607884]
    self.measure {
      (0 ..< 200_000).forEach {
        _ = self.benchmarkMemoryCache.object(forKey: keys[$0])
      }
    }
  }
}
