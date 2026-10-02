// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "WiciKit",
    platforms: [.macOS(.v13), .iOS(.v16)],
    products: [.library(name: "WiciKit", targets: ["WiciKit"])],
    targets: [
        .binaryTarget(name: "WiciFFI", path: "build/WiciFFI.xcframework"),
        .target(
            name: "WiciKit",
            dependencies: ["WiciFFI"],
            linkerSettings: [
                .linkedFramework("Security"),
                .linkedFramework("CoreFoundation"),
                .linkedFramework("SystemConfiguration"),
            ]
        ),
        .testTarget(name: "WiciKitTests", dependencies: ["WiciKit"]),
    ]
)
