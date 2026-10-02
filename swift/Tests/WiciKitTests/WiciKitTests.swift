import Foundation
import Testing
@testable import WiciKit

@Suite struct WiciKitTests {
    @Test func secretAndDeviceID() {
        let secret = WiciClient.generateSecret()
        #expect(secret.count == 64)
        #expect(WiciClient.deviceID(secret: secret)?.count == 43)
        #expect(WiciClient.deviceID(secret: Data([1])) == nil)
    }

    @Test func jsonRoundTrip() throws {
        let value: JSONValue = ["a": .array([.number(1), .bool(true), nil]), "b": "x"]
        #expect(try JSONValue.parse(value.text()) == value)
        #expect(value["b"]?.stringValue == "x")
        #expect(value["a"]?.arrayValue?.count == 3)
    }

    @Test func badSecretAndClosedClient() throws {
        #expect(throws: WiciError.self) {
            _ = try WiciClient(config: WiciConfig(serverURL: "ws://127.0.0.1:9", databasePath: "/tmp/x"), secret: Data())
        }
    }

    @Test func pairAndExchangeThroughServer() async throws {
        guard let url = ProcessInfo.processInfo.environment["WICI_TEST_SERVER_URL"] else { return }
        let a = try device(url)
        let b = try device(url)
        defer { a.close(); b.close() }

        let invitation = try await a.invite()
        #expect(try await b.join(link: invitation.link) == invitation.pair)
        _ = await first(a.events) { $0.type == "claimed" }
        try await a.approve(pair: invitation.pair)
        _ = await first(b.events) { $0.type == "pair" && $0.json["pair"]?["state"]?.stringValue == "active" }

        let body: JSONValue = ["type": "event", "stream": "01a0fd24-0000-7000-8000-000000000001", "data": "hello"]
        _ = try await b.send(pair: invitation.pair, body: body)
        let message = await first(a.events) { $0.type == "message" }
        #expect(message?.json["message"]?["body"]?["data"]?.stringValue == "hello")

        let artifact = try await a.upload(pair: invitation.pair, data: Data([1, 2, 3]), mediaType: "x/y")
        #expect(try await b.download(pair: invitation.pair, artifact: artifact) == Data([1, 2, 3]))

        await #expect(throws: WiciError.self) { try await a.approve(pair: "01a0fd24-0000-7000-8000-000000000009") }
        a.close()
        await #expect(throws: WiciError.self) { _ = try await a.call(["method": "pairs"]) }
    }

    private func device(_ url: String) throws -> WiciClient {
        let path = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString).path
        var config = WiciConfig(serverURL: url, databasePath: path)
        config.retryIntervalMilliseconds = 100
        config.reconnectMinMilliseconds = 20
        return try WiciClient(config: config, secret: WiciClient.generateSecret())
    }

    private func first(_ events: AsyncStream<WiciEvent>, where wanted: (WiciEvent) -> Bool) async -> WiciEvent? {
        for await event in events where wanted(event) { return event }
        return nil
    }
}
