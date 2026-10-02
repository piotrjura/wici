/* Wici client C ABI. Requests, results, and events are JSON strings. */
#ifndef WICI_H
#define WICI_H

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque client. */
typedef struct WiciClient WiciClient;

/* Receives JSON. `json` is valid only during the call. Runs on a Wici
 * thread; `context` must be safe to use from any thread. */
typedef void (*WiciCallback)(void *context, const char *json);

/* Writes 64 random secret bytes. Keep them in the Keychain. */
bool wici_device_secret_generate(uint8_t *out_secret64);

/* Device ID for a secret, or NULL. Free with wici_string_free. */
char *wici_device_id(const uint8_t *secret64);

/* Opens a client and starts connecting. Blocks while the database opens:
 * call off the main thread. On failure returns NULL and sets *error (free
 * with wici_string_free). `event_context` must stay valid until
 * wici_client_close returns. Config JSON: {"server_url", "database", ...}. */
WiciClient *wici_client_open(const char *config_json, const uint8_t *secret64,
                             WiciCallback on_event, void *event_context,
                             char **error);

/* Runs a JSON request such as {"method":"invite"}. `done` gets
 * {"ok": value} or {"error": {"kind", "message"}} exactly once. */
void wici_client_call(const WiciClient *client, const char *request_json,
                      WiciCallback done, void *context);

/* Stops and frees the client. Not from inside a callback. */
void wici_client_close(WiciClient *client);

/* Frees a string returned by this library. */
void wici_string_free(char *text);

#ifdef __cplusplus
}
#endif

#endif
