import Foundation
import Testing
@testable import WiciKit

@Suite(.timeLimit(.minutes(1))) struct LifecycleTests {
    @Test func overflowIsExplicitAndKeepsQueuedOrder() async throws {
        let sink = EventSink(capacity: 2)
        sink.receive(#"{"type":"message","id":1}"#)
        sink.receive(#"{"type":"message","id":2}"#)
        sink.receive(#"{"type":"message","id":3}"#)
        sink.finish()
        var iterator = sink.events.makeAsyncIterator()
        #expect(try await iterator.next()?.json["id"] == .number(1))
        #expect(try await iterator.next()?.json["id"] == .number(2))
        await #expect(throws: WiciError(kind: "event_overflow", message: "event buffer full; reopen and reconcile durable state")) {
            _ = try await iterator.next()
        }
    }

    @Test func droppedLiveDoesNotDisplaceDurableEvents() async throws {
        let sink = EventSink(capacity: 1)
        sink.receive(#"{"type":"message"}"#)
        sink.receive(#"{"type":"live"}"#)
        var iterator = sink.events.makeAsyncIterator()
        #expect(try await iterator.next()?.type == "message")
        sink.receive(#"{"type":"connected"}"#)
        sink.finish()
        #expect(try await iterator.next()?.type == "connected")
        #expect(try await iterator.next() == nil)
        sink.receive(#"{"type":"message"}"#)
        #expect(try await iterator.next() == nil)
    }

    @Test(arguments: ["broken", "{}"])
    func invalidEventsFailVisibly(text: String) async {
        let sink = EventSink(capacity: 1)
        sink.receive(text)
        await #expect(throws: WiciError.self) {
            for try await _ in sink.events {}
        }
    }

    @Test(arguments: [0, -1]) func invalidBufferCapacity(capacity: Int) {
        var config = WiciConfig(serverURL: "ws://127.0.0.1:9", databasePath: ":memory:")
        config.eventBufferCapacity = capacity
        #expect(throws: WiciError.self) {
            _ = try WiciClient(config: config, secret: WiciClient.generateSecret())
        }
    }

    @Test func closeRacesCallsWithoutLosingReplies() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let config = WiciConfig(serverURL: "ws://127.0.0.1:9", databasePath: directory.appendingPathComponent("client.db").path)
        let client = try WiciClient(config: config, secret: WiciClient.generateSecret())
        await withTaskGroup(of: Void.self) { group in
            for index in 0..<128 {
                group.addTask {
                    if index.isMultiple(of: 8) {
                        client.close()
                    } else {
                        do { _ = try await client.call(["method": "pairs"]) }
                        catch let error as WiciError { #expect(["closed", "outcome_unknown"].contains(error.kind)) }
                        catch { Issue.record(error) }
                    }
                }
            }
        }
        client.close()
        await #expect(throws: WiciError(kind: "closed", message: "client is closed")) {
            _ = try await client.call(["method": "pending"])
        }
        for try await _ in client.events {}
    }
}
