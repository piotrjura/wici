import Foundation
import WiciFFI

/// A failed Wici call.
public struct WiciError: Error, Sendable, Equatable {
    /// Machine-readable kind, for example `offline` or `pair_state`.
    public let kind: String
    /// Human-readable reason.
    public let message: String
}

/// Client settings.
public struct WiciConfig: Sendable {
    public var serverURL: String
    public var databasePath: String
    public var retryIntervalMilliseconds: Int?
    public var reconnectMinMilliseconds: Int?
    /// Maximum queued Swift events. Must be positive. Defaults to 256.
    public var eventBufferCapacity: Int = 256

    public init(serverURL: String, databasePath: String) {
        self.serverURL = serverURL
        self.databasePath = databasePath
    }

    func json() throws -> String {
        var object: [String: JSONValue] = [
            "server_url": .string(serverURL),
            "database": .string(databasePath),
        ]
        if let value = retryIntervalMilliseconds { object["retry_interval_ms"] = .number(Double(value)) }
        if let value = reconnectMinMilliseconds { object["reconnect_min_ms"] = .number(Double(value)) }
        return try JSONValue.object(object).text()
    }
}

/// An event from the client. `type` matches the Rust `Event` tag.
public struct WiciEvent: Sendable, Equatable {
    public let type: String
    public let json: JSONValue
}

/// Bounded callback bridge. AsyncThrowingStream synchronizes its continuation.
final class EventSink: Sendable {
    let events: AsyncThrowingStream<WiciEvent, Error>
    private let continuation: AsyncThrowingStream<WiciEvent, Error>.Continuation

    init(capacity: Int) {
        (events, continuation) = AsyncThrowingStream.makeStream(bufferingPolicy: .bufferingOldest(capacity))
    }

    func receive(_ text: String) {
        do {
            let value = try JSONValue.parse(text)
            guard let type = value["type"]?.stringValue else {
                continuation.finish(throwing: WiciError(kind: "invalid_event", message: "missing event type"))
                return
            }
            if case .dropped = continuation.yield(WiciEvent(type: type, json: value)), type != "live" {
                continuation.finish(throwing: WiciError(
                    kind: "event_overflow", message: "event buffer full; reopen and reconcile durable state"))
            }
        } catch {
            continuation.finish(throwing: WiciError(kind: "invalid_event", message: "invalid event JSON"))
        }
    }

    func finish() { continuation.finish() }
}

private final class ReplySink: Sendable {
    let continuation: CheckedContinuation<JSONValue, Error>
    init(_ continuation: CheckedContinuation<JSONValue, Error>) { self.continuation = continuation }
}

private func string(_ pointer: UnsafePointer<CChar>?) -> String {
    pointer.map { String(cString: $0) } ?? "null"
}

private let onEvent: WiciCallback = { context, json in
    guard let context else { return }
    let sink = Unmanaged<EventSink>.fromOpaque(context).takeUnretainedValue()
    sink.receive(string(json))
}

private let onReply: WiciCallback = { context, json in
    guard let context else { return }
    let sink = Unmanaged<ReplySink>.fromOpaque(context).takeRetainedValue()
    do {
        let value = try JSONValue.parse(string(json))
        if let error = value["error"] {
            sink.continuation.resume(throwing: WiciError(
                kind: error["kind"]?.stringValue ?? "unknown",
                message: error["message"]?.stringValue ?? ""))
        } else {
            sink.continuation.resume(returning: value["ok"] ?? .null)
        }
    } catch {
        sink.continuation.resume(throwing: WiciError(kind: "invalid_reply", message: "\(error)"))
    }
}

/// A Wici device client. Keep the device secret in the Keychain.
public final class WiciClient: @unchecked Sendable {
    private let lock = NSLock()
    private var handle: OpaquePointer?
    private let sink: Unmanaged<EventSink>

    /// Events in order; ends on close. Live updates may drop when full.
    /// Other overflow throws `event_overflow` after draining queued events.
    /// Reopen, read `pending` and current state, then resume consumption.
    /// Messages stay durable until explicitly marked `handled`.
    public let events: AsyncThrowingStream<WiciEvent, Error>

    /// New random device secret (64 bytes).
    public static func generateSecret() -> Data {
        var secret = Data(count: 64)
        _ = secret.withUnsafeMutableBytes { wici_device_secret_generate($0.bindMemory(to: UInt8.self).baseAddress) }
        return secret
    }

    /// Device ID for a secret, or `nil` if the secret is not 64 bytes.
    public static func deviceID(secret: Data) -> String? {
        guard secret.count == 64 else { return nil }
        guard let id = secret.withUnsafeBytes({ wici_device_id($0.bindMemory(to: UInt8.self).baseAddress) }) else {
            return nil
        }
        defer { wici_string_free(id) }
        return String(cString: id)
    }

    /// Opens the local database and starts connecting. Blocks briefly; call
    /// it off the main thread.
    public init(config: WiciConfig, secret: Data) throws {
        guard secret.count == 64 else { throw WiciError(kind: "invalid_secret", message: "secret must be 64 bytes") }
        guard config.eventBufferCapacity > 0 else {
            throw WiciError(kind: "invalid_config", message: "event buffer capacity must be positive")
        }
        let json = try config.json()
        let eventSink = EventSink(capacity: config.eventBufferCapacity)
        events = eventSink.events
        sink = Unmanaged.passRetained(eventSink)
        var error: UnsafeMutablePointer<CChar>?
        let opened = secret.withUnsafeBytes { bytes in
            wici_client_open(json, bytes.bindMemory(to: UInt8.self).baseAddress, onEvent, sink.toOpaque(), &error)
        }
        guard let opened else {
            sink.release()
            let message = error.map { String(cString: $0) } ?? "unknown"
            wici_string_free(error)
            throw WiciError(kind: "open", message: message)
        }
        handle = opened
    }

    deinit { close() }

    /// Stops the client and resolves in-flight calls before returning.
    /// Blocks during shutdown; call it off the main thread. Interrupted calls
    /// throw `outcome_unknown`; reconcile state before retrying any effect.
    public func close() {
        lock.withLock {
            guard let current = handle else { return }
            handle = nil
            wici_client_close(current)
            sink.takeUnretainedValue().finish()
            sink.release()
        }
    }

    /// Runs any JSON request, for example `["method": "pairs"]`.
    /// Task cancellation does not undo or cancel an accepted call. Closing
    /// interrupts its reply, but a durable write may already have committed.
    public func call(_ request: JSONValue) async throws -> JSONValue {
        let text = try request.text()
        return try await withCheckedThrowingContinuation { continuation in
            lock.withLock {
                guard let current = handle else {
                    continuation.resume(throwing: WiciError(kind: "closed", message: "client is closed"))
                    return
                }
                let context = Unmanaged.passRetained(ReplySink(continuation)).toOpaque()
                wici_client_call(current, text, onReply, context)
            }
        }
    }

    /// Creates an invitation. Show `link` as a QR code.
    public func invite() async throws -> (pair: String, link: String) {
        let result = try await call(["method": "invite"])
        return (result["pair"]?.stringValue ?? "", result["link"]?.stringValue ?? "")
    }

    /// Joins another device's invitation. Returns the pair ID.
    public func join(link: String) async throws -> String {
        try await call(["method": "join", "link": .string(link)])["pair"]?.stringValue ?? ""
    }

    /// Approves the device that claimed this device's invitation.
    public func approve(pair: String) async throws {
        _ = try await call(["method": "approve", "pair": .string(pair)])
    }

    /// Ends a pair.
    public func unpair(pair: String) async throws {
        _ = try await call(["method": "unpair", "pair": .string(pair)])
    }

    /// Sends a durable message. Returns its ID.
    public func send(pair: String, lane: String = "data", body: JSONValue) async throws -> String {
        let request: JSONValue = ["method": "send", "pair": .string(pair), "lane": .string(lane), "body": body]
        return try await call(request)["id"]?.stringValue ?? ""
    }

    /// Uploads and encrypts a file. Send the returned reference to the peer.
    public func upload(pair: String, data: Data, mediaType: String, name: String? = nil) async throws -> JSONValue {
        try await call([
            "method": "upload", "pair": .string(pair), "data": .string(data.base64EncodedString()),
            "media_type": .string(mediaType), "name": name.map(JSONValue.string) ?? .null,
        ])
    }

    /// Downloads and decrypts a file from its reference.
    public func download(pair: String, artifact: JSONValue) async throws -> Data {
        let result = try await call(["method": "download", "pair": .string(pair), "artifact": artifact])
        guard let text = result["data"]?.stringValue, let data = Data(base64Encoded: text) else {
            throw WiciError(kind: "invalid_reply", message: "missing data")
        }
        return data
    }
}
