// swift-tools-version:5.5
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
  name: "Kirogi",
  platforms: [
    .iOS(.v13)
  ],
  products: [
    .library(name: "Kirogi", targets: ["Kirogi"]),
  ],
  targets: [
    .systemLibrary(name: "CSQLite", path: "Sources/CSQLite"),
    .target(name: "Kirogi", dependencies: ["CSQLite"], path: "Sources/Core", linkerSettings: [.unsafeFlags(["-Xlinker", "-no_application_extension"])]),
    .testTarget(name: "KirogiTests", dependencies: ["Kirogi"], path: "Tests"),
  ],
  swiftLanguageVersions: [
    .v5
  ]
)
