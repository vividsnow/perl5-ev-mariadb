#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include "EVAPI.h"
#include <mysql.h>

#include "ngx_queue.h"

typedef struct ev_mariadb_s ev_mariadb_t;
typedef struct ev_mariadb_cb_s ev_mariadb_cb_t;
typedef struct ev_mariadb_send_s ev_mariadb_send_t;

typedef ev_mariadb_t* EV__MariaDB;
typedef struct ev_loop* EV__Loop;

#define EV_MARIADB_MAGIC 0xDEADBEEF
#define EV_MARIADB_FREED 0xFEEDFACE

enum ev_mariadb_state {
    STATE_IDLE,
    STATE_CONNECTING,
    STATE_SEND,
    STATE_READ_RESULT,
    STATE_STORE_RESULT,
    STATE_NEXT_RESULT,
    STATE_PING,
    STATE_CHANGE_USER,
    STATE_SELECT_DB,
    STATE_RESET_CONNECTION,
    STATE_STMT_PREPARE,
    STATE_STMT_EXECUTE,
    STATE_STMT_STORE,
    STATE_STMT_CLOSE,
    STATE_STMT_RESET,
};

struct ev_mariadb_s {
    unsigned int magic;
    struct ev_loop *loop;
    MYSQL *conn;

    ev_io    rio, wio;
    ev_timer timer;
    int      reading, writing, timing;
    int      fd;

    enum ev_mariadb_state state;
    char    *host, *user, *password, *database, *unix_socket;
    unsigned int port;

    ngx_queue_t cb_queue;       /* callbacks for sent queries (awaiting results) */
    ngx_queue_t send_queue;     /* queries waiting to be sent */
    int         pending_count;  /* total: send_queue + cb_queue */
    int         send_count;     /* queries sent, results not yet read */
    int         draining;       /* draining multi-result extras */

    /* current operation context */
    int          op_ret;
    MYSQL_RES   *op_result;
    MYSQL_STMT  *op_stmt;
    MYSQL       *op_conn_ret;
    my_bool      op_bool_ret;

    SV *on_connect;
    SV *on_error;

    int callback_depth;

    /* connection options (applied before mysql_real_connect_start) */
    unsigned int connect_timeout;
    unsigned int read_timeout;
    unsigned int write_timeout;
    int          compress;
    int          multi_statements;
    char        *charset;
    char        *init_command;
    char        *ssl_key;
    char        *ssl_cert;
    char        *ssl_ca;
    char        *ssl_cipher;
    int          ssl_verify_server_cert;
    unsigned long client_flags;
};

struct ev_mariadb_cb_s {
    SV          *cb;
    ngx_queue_t  queue;
};

struct ev_mariadb_send_s {
    char        *sql;
    unsigned long sql_len;
    SV          *cb;
    ngx_queue_t  queue;
};

#define MAX_PIPELINE_DEPTH 64

static void io_cb(EV_P_ ev_io *w, int revents);
static void timer_cb(EV_P_ ev_timer *w, int revents);
static void continue_operation(ev_mariadb_t *self, int events);
static void pipeline_advance(ev_mariadb_t *self);
static void on_next_result_done(ev_mariadb_t *self);
static void start_reading(ev_mariadb_t *self);
static void stop_reading(ev_mariadb_t *self);
static void start_writing(ev_mariadb_t *self);
static void stop_writing(ev_mariadb_t *self);
static void start_timer(ev_mariadb_t *self);
static void stop_timer(ev_mariadb_t *self);
static void update_watchers(ev_mariadb_t *self, int status);
static void emit_error(ev_mariadb_t *self, const char *msg);
static void cleanup_connection(ev_mariadb_t *self);
static int  check_destroyed(ev_mariadb_t *self);
static void drain_multi_result(ev_mariadb_t *self);

/* --- freelist for cb_queue entries --- */

static ev_mariadb_cb_t *cbt_freelist = NULL;

static ev_mariadb_cb_t* alloc_cbt(void) {
    ev_mariadb_cb_t *cbt;
    if (cbt_freelist) {
        cbt = cbt_freelist;
        cbt_freelist = *(ev_mariadb_cb_t **)cbt;
    } else {
        Newx(cbt, 1, ev_mariadb_cb_t);
    }
    return cbt;
}

static void release_cbt(ev_mariadb_cb_t *cbt) {
    *(ev_mariadb_cb_t **)cbt = cbt_freelist;
    cbt_freelist = cbt;
}

/* --- freelist for send_queue entries --- */

static ev_mariadb_send_t *send_freelist = NULL;

static ev_mariadb_send_t* alloc_send(void) {
    ev_mariadb_send_t *s;
    if (send_freelist) {
        s = send_freelist;
        send_freelist = *(ev_mariadb_send_t **)s;
    } else {
        Newx(s, 1, ev_mariadb_send_t);
    }
    return s;
}

static void release_send(ev_mariadb_send_t *s) {
    *(ev_mariadb_send_t **)s = send_freelist;
    send_freelist = s;
}

static void push_send(ev_mariadb_t *self, const char *sql, STRLEN sql_len, SV *cb) {
    ev_mariadb_send_t *s = alloc_send();
    Newx(s->sql, sql_len + 1, char);
    Copy(sql, s->sql, sql_len + 1, char);
    s->sql_len = (unsigned long)sql_len;
    s->cb = SvREFCNT_inc(cb);
    ngx_queue_insert_tail(&self->send_queue, &s->queue);
    self->pending_count++;
}

/* --- watcher helpers --- */

static void start_reading(ev_mariadb_t *self) {
    if (!self->reading && self->fd >= 0) {
        ev_io_start(self->loop, &self->rio);
        self->reading = 1;
    }
}

static void stop_reading(ev_mariadb_t *self) {
    if (self->reading) {
        ev_io_stop(self->loop, &self->rio);
        self->reading = 0;
    }
}

static void start_writing(ev_mariadb_t *self) {
    if (!self->writing && self->fd >= 0) {
        ev_io_start(self->loop, &self->wio);
        self->writing = 1;
    }
}

static void stop_writing(ev_mariadb_t *self) {
    if (self->writing) {
        ev_io_stop(self->loop, &self->wio);
        self->writing = 0;
    }
}

static void start_timer(ev_mariadb_t *self) {
    if (!self->timing) {
        unsigned int ms = mysql_get_timeout_value_ms(self->conn);
        ev_timer_set(&self->timer, ms / 1000.0, 0.0);
        ev_timer_start(self->loop, &self->timer);
        self->timing = 1;
    }
}

static void stop_timer(ev_mariadb_t *self) {
    if (self->timing) {
        ev_timer_stop(self->loop, &self->timer);
        self->timing = 0;
    }
}

static void update_watchers(ev_mariadb_t *self, int status) {
    if (status & MYSQL_WAIT_READ)  start_reading(self); else stop_reading(self);
    if (status & MYSQL_WAIT_WRITE) start_writing(self); else stop_writing(self);
    if (status & MYSQL_WAIT_TIMEOUT) start_timer(self); else stop_timer(self);
}

static int check_destroyed(ev_mariadb_t *self) {
    if (self->magic == EV_MARIADB_FREED &&
        self->callback_depth == 0) {
        Safefree(self);
        return 1;
    }
    return 0;
}

static void emit_error(ev_mariadb_t *self, const char *msg) {
    if (NULL == self->on_error) return;

    dSP;
    ENTER;
    SAVETMPS;
    PUSHMARK(SP);
    XPUSHs(sv_2mortal(newSVpv(msg, 0)));
    PUTBACK;

    call_sv(self->on_error, G_DISCARD | G_EVAL);
    if (SvTRUE(ERRSV)) {
        warn("EV::MariaDB: exception in error handler: %s", SvPV_nolen(ERRSV));
    }

    FREETMPS;
    LEAVE;
}

/* Pop and return the head callback from cb_queue. Caller must SvREFCNT_dec. */
static SV* pop_cb(ev_mariadb_t *self) {
    ngx_queue_t *q;
    ev_mariadb_cb_t *cbt;
    SV *cb;

    if (ngx_queue_empty(&self->cb_queue)) return NULL;

    q = ngx_queue_head(&self->cb_queue);
    cbt = ngx_queue_data(q, ev_mariadb_cb_t, queue);

    cb = cbt->cb;
    ngx_queue_remove(q);
    self->pending_count--;
    release_cbt(cbt);

    return cb;
}

/* Invoke a callback SV with args already on the stack. Decrements refcount. */
static void invoke_cb(SV *cb) {
    call_sv(cb, G_DISCARD | G_EVAL);
    if (SvTRUE(ERRSV)) {
        warn("EV::MariaDB: exception in callback: %s", SvPV_nolen(ERRSV));
    }
    SvREFCNT_dec(cb);
}

/* Returns 1 if self was freed (caller must not touch self). */
static int deliver_result(ev_mariadb_t *self, MYSQL_RES *res) {
    SV *cb = pop_cb(self);

    if (cb == NULL) {
        if (res) mysql_free_result(res);
        return 0;
    }

    self->callback_depth++;

    {
        dSP;
        ENTER;
        SAVETMPS;
        PUSHMARK(SP);

        if (res == NULL && mysql_field_count(self->conn) > 0) {
            PUSHs(&PL_sv_undef);
            PUSHs(sv_2mortal(newSVpv(mysql_error(self->conn), 0)));
        }
        else if (res != NULL) {
            unsigned int nrows = (unsigned int)mysql_num_rows(res);
            unsigned int ncols = mysql_num_fields(res);
            AV *rows = newAV();
            MYSQL_ROW row;
            unsigned long *lengths;

            if (nrows > 0) av_extend(rows, nrows - 1);
            while ((row = mysql_fetch_row(res)) != NULL) {
                AV *r = newAV();
                unsigned int c;
                lengths = mysql_fetch_lengths(res);
                if (ncols > 0) av_extend(r, ncols - 1);
                for (c = 0; c < ncols; c++) {
                    if (row[c] == NULL) {
                        av_push(r, newSV(0));
                    } else {
                        av_push(r, newSVpvn(row[c], lengths[c]));
                    }
                }
                av_push(rows, newRV_noinc((SV*)r));
            }
            PUSHs(sv_2mortal(newRV_noinc((SV*)rows)));
            mysql_free_result(res);
        }
        else {
            my_ulonglong affected = mysql_affected_rows(self->conn);
            PUSHs(sv_2mortal(newSVuv((UV)affected)));
        }

        PUTBACK;
        invoke_cb(cb);
        FREETMPS;
        LEAVE;
    }

    self->callback_depth--;
    return check_destroyed(self);
}

/* Returns 1 if self was freed. */
static int deliver_error(ev_mariadb_t *self, const char *errmsg) {
    SV *cb = pop_cb(self);
    if (cb == NULL) return 0;

    self->callback_depth++;

    {
        dSP;
        ENTER;
        SAVETMPS;
        PUSHMARK(SP);
        PUSHs(&PL_sv_undef);
        PUSHs(sv_2mortal(newSVpv(errmsg, 0)));
        PUTBACK;
        invoke_cb(cb);
        FREETMPS;
        LEAVE;
    }

    self->callback_depth--;
    return check_destroyed(self);
}

/* Returns 1 if self was freed. */
static int deliver_value(ev_mariadb_t *self, SV *val) {
    SV *cb = pop_cb(self);
    if (cb == NULL) return 0;

    self->callback_depth++;

    {
        dSP;
        ENTER;
        SAVETMPS;
        PUSHMARK(SP);
        PUSHs(val);
        PUTBACK;
        invoke_cb(cb);
        FREETMPS;
        LEAVE;
    }

    self->callback_depth--;
    return check_destroyed(self);
}

static void cleanup_connection(ev_mariadb_t *self) {
    stop_reading(self);
    stop_writing(self);
    stop_timer(self);
    self->fd = -1;
    self->state = STATE_IDLE;
    self->send_count = 0;
    self->draining = 0;

    if (self->op_result) {
        mysql_free_result(self->op_result);
        self->op_result = NULL;
    }

    if (self->conn) {
        MYSQL *conn = self->conn;
        self->conn = NULL;
        mysql_close(conn);
    }
}

static void cancel_pending(ev_mariadb_t *self, const char *errmsg) {
    self->callback_depth++;

    /* cancel unsent queries */
    while (!ngx_queue_empty(&self->send_queue)) {
        ngx_queue_t *q = ngx_queue_head(&self->send_queue);
        ev_mariadb_send_t *send = ngx_queue_data(q, ev_mariadb_send_t, queue);
        SV *cb = send->cb;
        ngx_queue_remove(q);
        Safefree(send->sql);
        release_send(send);
        self->pending_count--;

        {
            dSP;
            ENTER;
            SAVETMPS;
            PUSHMARK(SP);
            PUSHs(&PL_sv_undef);
            PUSHs(sv_2mortal(newSVpv(errmsg, 0)));
            PUTBACK;
            invoke_cb(cb);
            FREETMPS;
            LEAVE;
        }

        if (self->magic != EV_MARIADB_MAGIC) break;
    }

    /* cancel sent-but-not-received */
    while (!ngx_queue_empty(&self->cb_queue)) {
        SV *cb = pop_cb(self);
        if (cb == NULL) break;

        {
            dSP;
            ENTER;
            SAVETMPS;
            PUSHMARK(SP);
            PUSHs(&PL_sv_undef);
            PUSHs(sv_2mortal(newSVpv(errmsg, 0)));
            PUTBACK;
            invoke_cb(cb);
            FREETMPS;
            LEAVE;
        }

        if (self->magic != EV_MARIADB_MAGIC) break;
    }

    self->send_count = 0;
    self->callback_depth--;
}

static ev_mariadb_cb_t* push_cb(ev_mariadb_t *self, SV *cb) {
    ev_mariadb_cb_t *cbt = alloc_cbt();
    cbt->cb = SvREFCNT_inc(cb);
    ngx_queue_insert_tail(&self->cb_queue, &cbt->queue);
    self->pending_count++;
    return cbt;
}

/* Push cb to cb_queue, transferring ownership (no extra SvREFCNT_inc). */
static void push_cb_owned(ev_mariadb_t *self, SV *cb) {
    ev_mariadb_cb_t *cbt = alloc_cbt();
    cbt->cb = cb;
    ngx_queue_insert_tail(&self->cb_queue, &cbt->queue);
    /* pending_count already counted when push_send was called */
}

static SV* handler_accessor(SV **slot, SV *handler, int has_arg) {
    if (has_arg) {
        if (NULL != *slot) {
            SvREFCNT_dec(*slot);
            *slot = NULL;
        }
        if (NULL != handler && SvOK(handler) &&
            SvROK(handler) && SvTYPE(SvRV(handler)) == SVt_PVCV) {
            *slot = SvREFCNT_inc(handler);
        }
    }

    return (NULL != *slot)
        ? SvREFCNT_inc(*slot)
        : &PL_sv_undef;
}

/* --- Multi-result drain --- */

static void drain_multi_result(ev_mariadb_t *self) {
    int status;

    if (self->conn == NULL || !mysql_more_results(self->conn)) {
        self->draining = 0;
        self->state = STATE_IDLE;
        pipeline_advance(self);
        return;
    }

    self->draining = 1;
    status = mysql_next_result_start(&self->op_ret, self->conn);
    if (status != 0) {
        self->state = STATE_NEXT_RESULT;
        update_watchers(self, status);
        return;
    }
    on_next_result_done(self);
}

static void on_next_result_done(ev_mariadb_t *self) {
    int status;

    if (self->op_ret > 0) {
        /* error — stop draining, continue pipeline */
        self->draining = 0;
        self->state = STATE_IDLE;
        pipeline_advance(self);
        return;
    }

    if (self->op_ret == -1) {
        /* no more results */
        self->draining = 0;
        self->state = STATE_IDLE;
        pipeline_advance(self);
        return;
    }

    /* op_ret == 0: another result set is available */
    if (mysql_field_count(self->conn) > 0) {
        status = mysql_store_result_start(&self->op_result, self->conn);
        if (status != 0) {
            self->state = STATE_STORE_RESULT;
            update_watchers(self, status);
            return;
        }
        /* synchronous store — free and continue drain */
        if (self->op_result) {
            mysql_free_result(self->op_result);
            self->op_result = NULL;
        }
    }

    /* DML result or sync store done — check for more */
    if (mysql_more_results(self->conn)) {
        status = mysql_next_result_start(&self->op_ret, self->conn);
        if (status != 0) {
            self->state = STATE_NEXT_RESULT;
            update_watchers(self, status);
            return;
        }
        on_next_result_done(self);
        return;
    }

    self->draining = 0;
    self->state = STATE_IDLE;
    pipeline_advance(self);
}

/* --- Pipeline: send_query + read_query_result state machine --- */

/* Called when async send_query_cont completes */
static void on_send_done(ev_mariadb_t *self) {
    ngx_queue_t *q = ngx_queue_head(&self->send_queue);
    ev_mariadb_send_t *send = ngx_queue_data(q, ev_mariadb_send_t, queue);
    ngx_queue_remove(q);

    if (self->op_ret != 0) {
        /* send failed — fatal connection error */
        SV *cb = send->cb;
        Safefree(send->sql);
        release_send(send);
        self->pending_count--;

        self->state = STATE_IDLE;
        self->callback_depth++;
        {
            dSP;
            ENTER;
            SAVETMPS;
            PUSHMARK(SP);
            PUSHs(&PL_sv_undef);
            PUSHs(sv_2mortal(newSVpv(mysql_error(self->conn), 0)));
            PUTBACK;
            invoke_cb(cb);
            FREETMPS;
            LEAVE;
        }
        self->callback_depth--;
        if (check_destroyed(self)) return;
        cancel_pending(self, "send failed");
        if (self->magic != EV_MARIADB_MAGIC) return;
        cleanup_connection(self);
        return;
    }

    /* send succeeded — move cb to recv queue */
    push_cb_owned(self, send->cb);
    Safefree(send->sql);
    release_send(send);
    self->send_count++;

    self->state = STATE_IDLE;
    pipeline_advance(self);
}

/* Called when async read_query_result_cont completes */
static void on_read_result_done(ev_mariadb_t *self) {
    int status;

    if (self->op_bool_ret != 0) {
        /* query returned an error */
        self->send_count--;
        self->state = STATE_IDLE;
        if (deliver_error(self, mysql_error(self->conn))) return;
        if (self->state != STATE_IDLE) return;
        /* drain any remaining result sets */
        if (self->conn && mysql_more_results(self->conn)) {
            drain_multi_result(self);
            return;
        }
        pipeline_advance(self);
        return;
    }

    if (mysql_field_count(self->conn) > 0) {
        /* has result set — store it */
        status = mysql_store_result_start(&self->op_result, self->conn);
        if (status != 0) {
            self->state = STATE_STORE_RESULT;
            update_watchers(self, status);
            return;
        }
        /* synchronous store completion — fall through below */
    }

    /* DML or synchronous store_result completion */
    {
        MYSQL_RES *res = self->op_result;
        self->op_result = NULL;
        self->send_count--;
        self->state = STATE_IDLE;
        if (deliver_result(self, res)) return;
        if (self->state != STATE_IDLE) return;
        /* drain any extra result sets from multi-result queries */
        if (self->conn && mysql_more_results(self->conn)) {
            drain_multi_result(self);
            return;
        }
        pipeline_advance(self);
    }
}

/* Called when store_result_cont completes (pipeline text query path) */
static void on_store_result_done(ev_mariadb_t *self) {
    MYSQL_RES *res = self->op_result;
    self->op_result = NULL;

    if (self->draining) {
        /* draining multi-result: free and continue */
        if (res) mysql_free_result(res);
        if (self->conn && mysql_more_results(self->conn)) {
            int status = mysql_next_result_start(&self->op_ret, self->conn);
            if (status != 0) {
                self->state = STATE_NEXT_RESULT;
                update_watchers(self, status);
                return;
            }
            on_next_result_done(self);
            return;
        }
        self->draining = 0;
        self->state = STATE_IDLE;
        pipeline_advance(self);
        return;
    }

    /* normal path */
    self->send_count--;
    self->state = STATE_IDLE;

    if (deliver_result(self, res)) return;
    if (self->state != STATE_IDLE) return;
    /* drain any extra result sets */
    if (self->conn && mysql_more_results(self->conn)) {
        drain_multi_result(self);
        return;
    }
    pipeline_advance(self);
}

/*
 * Pipeline orchestrator. Called when state == IDLE.
 * Phase 1: send all queued queries via mysql_send_query.
 * Phase 2: read next result via mysql_read_query_result.
 */
static void pipeline_advance(ev_mariadb_t *self) {
    int status;

    /* Ensure clean watcher state — previous operations may have left
     * watchers active after completing synchronously within their
     * done handlers. Without this, subsequent operations that need
     * the same watcher direction would skip ev_io_start. */
    stop_reading(self);
    stop_writing(self);
    stop_timer(self);

send_phase:
    /* Phase 1: send up to MAX_PIPELINE_DEPTH queries */
    while (!ngx_queue_empty(&self->send_queue) &&
           self->send_count < MAX_PIPELINE_DEPTH) {
        ngx_queue_t *q = ngx_queue_head(&self->send_queue);
        ev_mariadb_send_t *send = ngx_queue_data(q, ev_mariadb_send_t, queue);

        status = mysql_send_query_start(&self->op_ret, self->conn,
            send->sql, send->sql_len);

        if (status != 0) {
            /* need async IO to finish sending */
            self->state = STATE_SEND;
            update_watchers(self, status);
            return;
        }

        /* synchronous send completion */
        ngx_queue_remove(q);

        if (self->op_ret != 0) {
            /* send failed — fatal */
            SV *cb = send->cb;
            Safefree(send->sql);
            release_send(send);
            self->pending_count--;

            self->callback_depth++;
            {
                dSP;
                ENTER;
                SAVETMPS;
                PUSHMARK(SP);
                PUSHs(&PL_sv_undef);
                PUSHs(sv_2mortal(newSVpv(mysql_error(self->conn), 0)));
                PUTBACK;
                invoke_cb(cb);
                FREETMPS;
                LEAVE;
            }
            self->callback_depth--;
            if (check_destroyed(self)) return;
            cancel_pending(self, "send failed");
            if (self->magic != EV_MARIADB_MAGIC) return;
            cleanup_connection(self);
            return;
        }

        /* send succeeded */
        push_cb_owned(self, send->cb);
        Safefree(send->sql);
        release_send(send);
        self->send_count++;
    }

    /* Phase 2: read next result */
    while (self->send_count > 0) {
        status = mysql_read_query_result_start(&self->op_bool_ret, self->conn);

        if (status != 0) {
            self->state = STATE_READ_RESULT;
            update_watchers(self, status);
            return;
        }

        /* synchronous read completion */
        if (self->op_bool_ret != 0) {
            /* query error */
            self->send_count--;
            if (deliver_error(self, mysql_error(self->conn))) return;
            /* callback may have started a new operation */
            if (self->state != STATE_IDLE) return;
            if (self->conn && mysql_more_results(self->conn)) {
                drain_multi_result(self);
                return;
            }
            continue;
        }

        if (mysql_field_count(self->conn) > 0) {
            /* has result set */
            status = mysql_store_result_start(&self->op_result, self->conn);
            if (status != 0) {
                self->state = STATE_STORE_RESULT;
                update_watchers(self, status);
                return;
            }
        }

        {
            MYSQL_RES *res = self->op_result;
            self->op_result = NULL;
            self->send_count--;
            if (deliver_result(self, res)) return;
            /* callback may have started a new operation */
            if (self->state != STATE_IDLE) return;
            if (self->conn && mysql_more_results(self->conn)) {
                drain_multi_result(self);
                return;
            }
            /* loop back to check send_queue (callback may have queued more) */
            if (!ngx_queue_empty(&self->send_queue)) goto send_phase;
        }
    }

    self->state = STATE_IDLE;
}

/* --- Connection --- */

static void on_connect_done(ev_mariadb_t *self) {
    self->state = STATE_IDLE;

    if (self->op_conn_ret == NULL) {
        char errbuf[512];
        strncpy(errbuf, mysql_error(self->conn), sizeof(errbuf) - 1);
        errbuf[sizeof(errbuf) - 1] = '\0';
        self->callback_depth++;
        emit_error(self, errbuf);
        self->callback_depth--;
        if (check_destroyed(self)) return;
        cancel_pending(self, errbuf);
        if (self->magic != EV_MARIADB_MAGIC) return;
        cleanup_connection(self);
        return;
    }

    /* connected — reinit watchers for normal IO */
    stop_reading(self);
    stop_writing(self);
    stop_timer(self);

    self->fd = mysql_get_socket(self->conn);

    if (self->fd < 0) {
        self->callback_depth++;
        emit_error(self, "mysql_get_socket returned invalid fd");
        self->callback_depth--;
        if (check_destroyed(self)) return;
        cancel_pending(self, "invalid fd");
        if (self->magic != EV_MARIADB_MAGIC) return;
        cleanup_connection(self);
        return;
    }

    ev_io_init(&self->rio, io_cb, self->fd, EV_READ);
    self->rio.data = (void *)self;
    ev_io_init(&self->wio, io_cb, self->fd, EV_WRITE);
    self->wio.data = (void *)self;

    if (NULL != self->on_connect) {
        self->callback_depth++;

        {
            dSP;
            ENTER;
            SAVETMPS;
            PUSHMARK(SP);
            PUTBACK;

            call_sv(self->on_connect, G_DISCARD | G_EVAL);
            if (SvTRUE(ERRSV)) {
                warn("EV::MariaDB: exception in connect handler: %s", SvPV_nolen(ERRSV));
            }

            FREETMPS;
            LEAVE;
        }

        self->callback_depth--;
        if (check_destroyed(self)) return;
    }

    /* start pipeline if queries were queued during connection */
    if (!ngx_queue_empty(&self->send_queue))
        pipeline_advance(self);
}

/* --- Prepared statements --- */

static void on_stmt_prepare_done(ev_mariadb_t *self) {
    MYSQL_STMT *stmt = self->op_stmt;
    self->op_stmt = NULL;
    self->state = STATE_IDLE;

    if (self->op_ret != 0) {
        char errbuf[512];
        strncpy(errbuf, mysql_stmt_error(stmt), sizeof(errbuf) - 1);
        errbuf[sizeof(errbuf) - 1] = '\0';
        mysql_stmt_close(stmt);
        if (deliver_error(self, errbuf)) return;
        if (self->state == STATE_IDLE && !ngx_queue_empty(&self->send_queue))
            pipeline_advance(self);
        return;
    }

    if (deliver_value(self, sv_2mortal(newSViv(PTR2IV(stmt))))) return;
    if (self->state == STATE_IDLE && !ngx_queue_empty(&self->send_queue))
        pipeline_advance(self);
}

static void on_stmt_execute_done(ev_mariadb_t *self) {
    MYSQL_STMT *stmt = self->op_stmt;
    self->op_stmt = NULL;
    self->state = STATE_IDLE;

    if (self->op_ret != 0) {
        if (deliver_error(self, mysql_stmt_error(stmt))) return;
        if (self->state == STATE_IDLE && !ngx_queue_empty(&self->send_queue))
            pipeline_advance(self);
        return;
    }

    {
        MYSQL_RES *meta = mysql_stmt_result_metadata(stmt);
        SV *cb = pop_cb(self);
        if (cb == NULL) {
            if (meta) mysql_free_result(meta);
            return;
        }

        self->callback_depth++;
        {
            dSP;
            ENTER;
            SAVETMPS;
            PUSHMARK(SP);

            if (meta == NULL) {
                my_ulonglong affected = mysql_stmt_affected_rows(stmt);
                PUSHs(sv_2mortal(newSVuv((UV)affected)));
            }
            else {
                unsigned int ncols = mysql_num_fields(meta);
                MYSQL_FIELD *fields = mysql_fetch_fields(meta);
                MYSQL_BIND *bind;
                unsigned long *lengths;
                my_bool *is_null;
                char **buffers;
                unsigned long *bufsizes;
                unsigned int c;
                AV *rows = newAV();
                int fetch_ret;

                Newxz(bind, ncols, MYSQL_BIND);
                Newx(lengths, ncols, unsigned long);
                Newx(is_null, ncols, my_bool);
                Newx(buffers, ncols, char *);
                Newx(bufsizes, ncols, unsigned long);

                for (c = 0; c < ncols; c++) {
                    unsigned long buflen = fields[c].max_length;
                    if (buflen < 256) buflen = 256;
                    Newx(buffers[c], buflen, char);
                    bufsizes[c] = buflen;
                    bind[c].buffer_type = MYSQL_TYPE_STRING;
                    bind[c].buffer = buffers[c];
                    bind[c].buffer_length = buflen;
                    bind[c].length = &lengths[c];
                    bind[c].is_null = &is_null[c];
                }

                if (mysql_stmt_bind_result(stmt, bind)) {
                    for (c = 0; c < ncols; c++) Safefree(buffers[c]);
                    Safefree(bufsizes);
                    Safefree(buffers);
                    Safefree(is_null);
                    Safefree(lengths);
                    Safefree(bind);
                    mysql_free_result(meta);

                    PUSHs(&PL_sv_undef);
                    PUSHs(sv_2mortal(newSVpv(mysql_stmt_error(stmt), 0)));
                    goto invoke;
                }

                while ((fetch_ret = mysql_stmt_fetch(stmt)) == 0 ||
                       fetch_ret == MYSQL_DATA_TRUNCATED) {
                    AV *row = newAV();
                    if (ncols > 0) av_extend(row, ncols - 1);
                    for (c = 0; c < ncols; c++) {
                        if (is_null[c]) {
                            av_push(row, newSV(0));
                        } else if (lengths[c] > bufsizes[c]) {
                            /* truncated — refetch this column */
                            MYSQL_BIND rebind;
                            char *bigbuf;
                            unsigned long biglen = lengths[c];
                            Newx(bigbuf, biglen, char);
                            Zero(&rebind, 1, MYSQL_BIND);
                            rebind.buffer_type = MYSQL_TYPE_STRING;
                            rebind.buffer = bigbuf;
                            rebind.buffer_length = biglen;
                            rebind.length = &biglen;
                            rebind.is_null = &is_null[c];
                            mysql_stmt_fetch_column(stmt, &rebind, c, 0);
                            av_push(row, newSVpvn(bigbuf, biglen));
                            Safefree(bigbuf);
                        } else {
                            av_push(row, newSVpvn(buffers[c], lengths[c]));
                        }
                    }
                    av_push(rows, newRV_noinc((SV*)row));
                }

                for (c = 0; c < ncols; c++) Safefree(buffers[c]);
                Safefree(bufsizes);
                Safefree(buffers);
                Safefree(is_null);
                Safefree(lengths);
                Safefree(bind);
                mysql_free_result(meta);

                PUSHs(sv_2mortal(newRV_noinc((SV*)rows)));
            }

        invoke:
            PUTBACK;
            invoke_cb(cb);
            FREETMPS;
            LEAVE;
        }
        self->callback_depth--;
        if (check_destroyed(self)) return;
        if (self->state == STATE_IDLE && !ngx_queue_empty(&self->send_queue))
            pipeline_advance(self);
    }
}

static void on_stmt_store_done(ev_mariadb_t *self) {
    if (self->op_ret != 0) {
        MYSQL_STMT *stmt = self->op_stmt;
        self->op_stmt = NULL;
        self->state = STATE_IDLE;
        if (deliver_error(self, mysql_stmt_error(stmt))) return;
        if (self->state == STATE_IDLE && !ngx_queue_empty(&self->send_queue))
            pipeline_advance(self);
        return;
    }
    on_stmt_execute_done(self);
}

static void on_stmt_close_done(ev_mariadb_t *self) {
    self->op_stmt = NULL;
    self->state = STATE_IDLE;
    if (deliver_value(self, sv_2mortal(newSViv(1)))) return;
    if (self->state == STATE_IDLE && !ngx_queue_empty(&self->send_queue))
        pipeline_advance(self);
}

static void on_stmt_reset_done(ev_mariadb_t *self) {
    MYSQL_STMT *stmt = self->op_stmt;
    self->op_stmt = NULL;
    self->state = STATE_IDLE;
    if (self->op_bool_ret != 0) {
        if (deliver_error(self, mysql_stmt_error(stmt))) return;
    } else {
        if (deliver_value(self, sv_2mortal(newSViv(1)))) return;
    }
    if (self->state == STATE_IDLE && !ngx_queue_empty(&self->send_queue))
        pipeline_advance(self);
}

/* --- Async utility operation done handlers --- */

static void on_ping_done(ev_mariadb_t *self) {
    self->state = STATE_IDLE;
    if (self->op_ret != 0) {
        if (deliver_error(self, mysql_error(self->conn))) return;
    } else {
        if (deliver_value(self, sv_2mortal(newSViv(1)))) return;
    }
    if (self->state == STATE_IDLE && !ngx_queue_empty(&self->send_queue))
        pipeline_advance(self);
}

static void on_select_db_done(ev_mariadb_t *self) {
    self->state = STATE_IDLE;
    if (self->op_ret != 0) {
        if (deliver_error(self, mysql_error(self->conn))) return;
    } else {
        if (deliver_value(self, sv_2mortal(newSViv(1)))) return;
    }
    if (self->state == STATE_IDLE && !ngx_queue_empty(&self->send_queue))
        pipeline_advance(self);
}

static void on_change_user_done(ev_mariadb_t *self) {
    self->state = STATE_IDLE;
    if (self->op_bool_ret != 0) {
        if (deliver_error(self, mysql_error(self->conn))) return;
    } else {
        if (deliver_value(self, sv_2mortal(newSViv(1)))) return;
    }
    if (self->state == STATE_IDLE && !ngx_queue_empty(&self->send_queue))
        pipeline_advance(self);
}

static void on_reset_connection_done(ev_mariadb_t *self) {
    self->state = STATE_IDLE;
    if (self->op_ret != 0) {
        if (deliver_error(self, mysql_error(self->conn))) return;
    } else {
        if (deliver_value(self, sv_2mortal(newSViv(1)))) return;
    }
    if (self->state == STATE_IDLE && !ngx_queue_empty(&self->send_queue))
        pipeline_advance(self);
}

/* --- Main continuation dispatcher --- */

static void continue_operation(ev_mariadb_t *self, int events) {
    int status;

    switch (self->state) {
    case STATE_CONNECTING:
        status = mysql_real_connect_cont(&self->op_conn_ret, self->conn, events);
        if (status == 0) {
            on_connect_done(self);
        } else {
            update_watchers(self, status);
        }
        break;

    case STATE_SEND:
        status = mysql_send_query_cont(&self->op_ret, self->conn, events);
        if (status == 0) {
            on_send_done(self);
        } else {
            update_watchers(self, status);
        }
        break;

    case STATE_READ_RESULT:
        status = mysql_read_query_result_cont(&self->op_bool_ret, self->conn, events);
        if (status == 0) {
            on_read_result_done(self);
        } else {
            update_watchers(self, status);
        }
        break;

    case STATE_STORE_RESULT:
        status = mysql_store_result_cont(&self->op_result, self->conn, events);
        if (status == 0) {
            on_store_result_done(self);
        } else {
            update_watchers(self, status);
        }
        break;

    case STATE_NEXT_RESULT:
        status = mysql_next_result_cont(&self->op_ret, self->conn, events);
        if (status == 0) {
            on_next_result_done(self);
        } else {
            update_watchers(self, status);
        }
        break;

    case STATE_STMT_PREPARE:
        status = mysql_stmt_prepare_cont(&self->op_ret, self->op_stmt, events);
        if (status == 0) {
            on_stmt_prepare_done(self);
        } else {
            update_watchers(self, status);
        }
        break;

    case STATE_STMT_EXECUTE:
        status = mysql_stmt_execute_cont(&self->op_ret, self->op_stmt, events);
        if (status == 0) {
            if (self->op_ret != 0) {
                on_stmt_execute_done(self);
            } else {
                self->state = STATE_STMT_STORE;
                status = mysql_stmt_store_result_start(&self->op_ret, self->op_stmt);
                if (status == 0) {
                    on_stmt_store_done(self);
                } else {
                    update_watchers(self, status);
                }
            }
        } else {
            update_watchers(self, status);
        }
        break;

    case STATE_STMT_STORE:
        status = mysql_stmt_store_result_cont(&self->op_ret, self->op_stmt, events);
        if (status == 0) {
            on_stmt_store_done(self);
        } else {
            update_watchers(self, status);
        }
        break;

    case STATE_STMT_CLOSE:
        status = mysql_stmt_close_cont(&self->op_bool_ret, self->op_stmt, events);
        if (status == 0) {
            on_stmt_close_done(self);
        } else {
            update_watchers(self, status);
        }
        break;

    case STATE_STMT_RESET:
        status = mysql_stmt_reset_cont(&self->op_bool_ret, self->op_stmt, events);
        if (status == 0) {
            on_stmt_reset_done(self);
        } else {
            update_watchers(self, status);
        }
        break;

    case STATE_PING:
        status = mysql_ping_cont(&self->op_ret, self->conn, events);
        if (status == 0) {
            on_ping_done(self);
        } else {
            update_watchers(self, status);
        }
        break;

    case STATE_CHANGE_USER:
        status = mysql_change_user_cont(&self->op_bool_ret, self->conn, events);
        if (status == 0) {
            on_change_user_done(self);
        } else {
            update_watchers(self, status);
        }
        break;

    case STATE_SELECT_DB:
        status = mysql_select_db_cont(&self->op_ret, self->conn, events);
        if (status == 0) {
            on_select_db_done(self);
        } else {
            update_watchers(self, status);
        }
        break;

    case STATE_RESET_CONNECTION:
        status = mysql_reset_connection_cont(&self->op_ret, self->conn, events);
        if (status == 0) {
            on_reset_connection_done(self);
        } else {
            update_watchers(self, status);
        }
        break;

    case STATE_IDLE:
        break;

    default:
        break;
    }
}

static void io_cb(EV_P_ ev_io *w, int revents) {
    ev_mariadb_t *self = (ev_mariadb_t *)w->data;
    int events = 0;
    (void)loop;

    if (self == NULL || self->magic != EV_MARIADB_MAGIC) return;
    if (self->conn == NULL) return;

    if (revents & EV_READ)  events |= MYSQL_WAIT_READ;
    if (revents & EV_WRITE) events |= MYSQL_WAIT_WRITE;

    continue_operation(self, events);
}

static void timer_cb(EV_P_ ev_timer *w, int revents) {
    ev_mariadb_t *self = (ev_mariadb_t *)w->data;
    (void)loop;
    (void)revents;

    if (self == NULL || self->magic != EV_MARIADB_MAGIC) return;
    if (self->conn == NULL) return;

    self->timing = 0;
    continue_operation(self, MYSQL_WAIT_TIMEOUT);
}

static char* safe_strdup(const char *s) {
    char *d;
    size_t len;
    if (!s) return NULL;
    len = strlen(s);
    Newx(d, len + 1, char);
    Copy(s, d, len + 1, char);
    return d;
}

static void free_option_strings(ev_mariadb_t *self) {
    if (self->charset)     { Safefree(self->charset);     self->charset = NULL; }
    if (self->init_command) { Safefree(self->init_command); self->init_command = NULL; }
    if (self->ssl_key)     { Safefree(self->ssl_key);     self->ssl_key = NULL; }
    if (self->ssl_cert)    { Safefree(self->ssl_cert);    self->ssl_cert = NULL; }
    if (self->ssl_ca)      { Safefree(self->ssl_ca);      self->ssl_ca = NULL; }
    if (self->ssl_cipher)  { Safefree(self->ssl_cipher);  self->ssl_cipher = NULL; }
}

static void apply_options(ev_mariadb_t *self) {
    MYSQL *conn = self->conn;
    unsigned long flags = self->client_flags;

    if (self->connect_timeout > 0)
        mysql_options(conn, MYSQL_OPT_CONNECT_TIMEOUT, &self->connect_timeout);
    if (self->read_timeout > 0)
        mysql_options(conn, MYSQL_OPT_READ_TIMEOUT, &self->read_timeout);
    if (self->write_timeout > 0)
        mysql_options(conn, MYSQL_OPT_WRITE_TIMEOUT, &self->write_timeout);
    if (self->compress)
        mysql_options(conn, MYSQL_OPT_COMPRESS, NULL);
    if (self->charset)
        mysql_options(conn, MYSQL_SET_CHARSET_NAME, self->charset);
    if (self->init_command)
        mysql_options(conn, MYSQL_INIT_COMMAND, self->init_command);
    if (self->ssl_ca || self->ssl_cert || self->ssl_key || self->ssl_cipher)
        mysql_ssl_set(conn, self->ssl_key, self->ssl_cert, self->ssl_ca, NULL, self->ssl_cipher);
    if (self->ssl_verify_server_cert) {
        my_bool val = 1;
        mysql_options(conn, MYSQL_OPT_SSL_VERIFY_SERVER_CERT, &val);
    }
    if (self->multi_statements)
        flags |= CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS;

    self->client_flags = flags;
}

MODULE = EV::MariaDB  PACKAGE = EV::MariaDB

BOOT:
{
    I_EV_API("EV::MariaDB");
}

EV::MariaDB
_new(char *class, EV::Loop loop)
CODE:
{
    PERL_UNUSED_VAR(class);
    Newxz(RETVAL, 1, ev_mariadb_t);
    RETVAL->magic = EV_MARIADB_MAGIC;
    RETVAL->loop = loop;
    RETVAL->fd = -1;
    RETVAL->state = STATE_IDLE;
    ngx_queue_init(&RETVAL->cb_queue);
    ngx_queue_init(&RETVAL->send_queue);

    ev_init(&RETVAL->timer, timer_cb);
    RETVAL->timer.data = (void *)RETVAL;
}
OUTPUT:
    RETVAL

void
DESTROY(EV::MariaDB self)
CODE:
{
    if (self->magic != EV_MARIADB_MAGIC) {
        if (self->magic == EV_MARIADB_FREED) return;
        return;
    }

    self->magic = EV_MARIADB_FREED;

    stop_reading(self);
    stop_writing(self);
    stop_timer(self);

    if (PL_dirty) {
        /* global destruction — free C resources, don't invoke callbacks */
        while (!ngx_queue_empty(&self->send_queue)) {
            ngx_queue_t *q = ngx_queue_head(&self->send_queue);
            ev_mariadb_send_t *send = ngx_queue_data(q, ev_mariadb_send_t, queue);
            ngx_queue_remove(q);
            Safefree(send->sql);
            SvREFCNT_dec(send->cb);
            release_send(send);
        }
        while (!ngx_queue_empty(&self->cb_queue)) {
            ngx_queue_t *q = ngx_queue_head(&self->cb_queue);
            ev_mariadb_cb_t *cbt = ngx_queue_data(q, ev_mariadb_cb_t, queue);
            ngx_queue_remove(q);
            SvREFCNT_dec(cbt->cb);
            release_cbt(cbt);
        }
        if (self->conn) mysql_close(self->conn);
        if (self->host) Safefree(self->host);
        if (self->user) Safefree(self->user);
        if (self->password) Safefree(self->password);
        if (self->database) Safefree(self->database);
        if (self->unix_socket) Safefree(self->unix_socket);
        free_option_strings(self);
        Safefree(self);
        return;
    }

    cancel_pending(self, "object destroyed");

    if (self->op_result) {
        mysql_free_result(self->op_result);
        self->op_result = NULL;
    }

    {
        MYSQL *conn = self->conn;
        self->conn = NULL;
        self->loop = NULL;
        self->fd = -1;
        if (conn) mysql_close(conn);
    }

    if (NULL != self->on_connect) {
        SvREFCNT_dec(self->on_connect);
        self->on_connect = NULL;
    }
    if (NULL != self->on_error) {
        SvREFCNT_dec(self->on_error);
        self->on_error = NULL;
    }
    if (self->host) { Safefree(self->host); self->host = NULL; }
    if (self->user) { Safefree(self->user); self->user = NULL; }
    if (self->password) { Safefree(self->password); self->password = NULL; }
    if (self->database) { Safefree(self->database); self->database = NULL; }
    if (self->unix_socket) { Safefree(self->unix_socket); self->unix_socket = NULL; }
    free_option_strings(self);

    if (self->callback_depth > 0) {
        /* deferred free: check_destroyed will Safefree when depth hits 0 */
    }
    else {
        Safefree(self);
    }
}

void
connect(EV::MariaDB self, const char *host, const char *user, const char *password, const char *database, unsigned int port = 3306, SV *unix_socket_sv = NULL)
CODE:
{
    int status;
    const char *usock = NULL;

    if (NULL != self->conn) {
        croak("already connected");
    }

    if (self->host) Safefree(self->host);
    if (self->user) Safefree(self->user);
    if (self->password) Safefree(self->password);
    if (self->database) Safefree(self->database);
    if (self->unix_socket) Safefree(self->unix_socket);

    self->host = safe_strdup(host);
    self->user = safe_strdup(user);
    self->password = safe_strdup(password);
    self->database = safe_strdup(database);
    self->port = port;
    self->unix_socket = NULL;

    if (unix_socket_sv && SvOK(unix_socket_sv)) {
        usock = SvPV_nolen(unix_socket_sv);
        self->unix_socket = safe_strdup(usock);
    }

    self->conn = mysql_init(NULL);
    if (NULL == self->conn) {
        croak("mysql_init failed");
    }

    mysql_options(self->conn, MYSQL_OPT_NONBLOCK, 0);
    apply_options(self);

    self->state = STATE_CONNECTING;
    status = mysql_real_connect_start(&self->op_conn_ret, self->conn,
        self->host, self->user, self->password, self->database,
        self->port, self->unix_socket, self->client_flags);

    if (status == 0) {
        on_connect_done(self);
    } else {
        self->fd = mysql_get_socket(self->conn);
        if (self->fd < 0) {
            mysql_close(self->conn);
            self->conn = NULL;
            self->state = STATE_IDLE;
            croak("mysql_get_socket returned invalid fd");
        }

        ev_io_init(&self->rio, io_cb, self->fd, EV_READ);
        self->rio.data = (void *)self;
        ev_io_init(&self->wio, io_cb, self->fd, EV_WRITE);
        self->wio.data = (void *)self;

        update_watchers(self, status);
    }
}

void
reset(EV::MariaDB self)
CODE:
{
    int status;

    if (NULL == self->host) {
        croak("no previous connection to reset");
    }

    cancel_pending(self, "connection reset");
    cleanup_connection(self);

    self->conn = mysql_init(NULL);
    if (NULL == self->conn) {
        croak("mysql_init failed");
    }

    mysql_options(self->conn, MYSQL_OPT_NONBLOCK, 0);
    apply_options(self);

    self->state = STATE_CONNECTING;
    status = mysql_real_connect_start(&self->op_conn_ret, self->conn,
        self->host, self->user, self->password, self->database,
        self->port, self->unix_socket, self->client_flags);

    if (status == 0) {
        on_connect_done(self);
    } else {
        self->fd = mysql_get_socket(self->conn);
        if (self->fd < 0) {
            mysql_close(self->conn);
            self->conn = NULL;
            self->state = STATE_IDLE;
            croak("mysql_get_socket returned invalid fd");
        }

        ev_io_init(&self->rio, io_cb, self->fd, EV_READ);
        self->rio.data = (void *)self;
        ev_io_init(&self->wio, io_cb, self->fd, EV_WRITE);
        self->wio.data = (void *)self;

        update_watchers(self, status);
    }
}

void
finish(EV::MariaDB self)
CODE:
{
    cancel_pending(self, "connection finished");
    cleanup_connection(self);
}

SV*
on_connect(EV::MariaDB self, SV *handler = NULL)
CODE:
{
    RETVAL = handler_accessor(&self->on_connect, handler, items > 1);
}
OUTPUT:
    RETVAL

SV*
on_error(EV::MariaDB self, SV *handler = NULL)
CODE:
{
    RETVAL = handler_accessor(&self->on_error, handler, items > 1);
}
OUTPUT:
    RETVAL

void
query(EV::MariaDB self, SV *sql_sv, SV *cb)
CODE:
{
    STRLEN sql_len;
    const char *sql;

    if (NULL == self->conn && self->state != STATE_CONNECTING) {
        croak("not connected");
    }
    if (self->state >= STATE_STMT_PREPARE) {
        croak("cannot queue query during prepared statement operation");
    }
    if (!(SvROK(cb) && SvTYPE(SvRV(cb)) == SVt_PVCV)) {
        croak("callback must be a CODE reference");
    }

    sql = SvPV(sql_sv, sql_len);
    push_send(self, sql, sql_len, cb);

    if (self->state == STATE_IDLE && self->callback_depth == 0)
        pipeline_advance(self);
}

void
prepare(EV::MariaDB self, SV *sql_sv, SV *cb)
PREINIT:
    STRLEN sql_len;
    const char *sql;
CODE:
{
    int status;
    MYSQL_STMT *stmt;

    if (NULL == self->conn || self->state == STATE_CONNECTING) {
        croak("not connected");
    }
    if (self->state != STATE_IDLE) {
        croak("another operation is in progress");
    }
    if (self->send_count > 0) {
        croak("cannot start operation while pipeline results are pending");
    }
    if (!(SvROK(cb) && SvTYPE(SvRV(cb)) == SVt_PVCV)) {
        croak("callback must be a CODE reference");
    }

    stmt = mysql_stmt_init(self->conn);
    if (NULL == stmt) {
        croak("mysql_stmt_init failed");
    }
    {
        my_bool update_max = 1;
        mysql_stmt_attr_set(stmt, STMT_ATTR_UPDATE_MAX_LENGTH, &update_max);
    }

    sql = SvPV(sql_sv, sql_len);
    push_cb(self, cb);
    self->op_stmt = stmt;

    self->state = STATE_STMT_PREPARE;
    status = mysql_stmt_prepare_start(&self->op_ret, stmt, sql, (unsigned long)sql_len);
    if (status == 0) {
        on_stmt_prepare_done(self);
    } else {
        update_watchers(self, status);
    }
}

void
execute(EV::MariaDB self, IV stmt_iv, SV *params_ref, SV *cb)
PREINIT:
    MYSQL_STMT *stmt;
    AV *params;
    int nparams;
    MYSQL_BIND *bind_params;
    int i, status;
CODE:
{
    if (NULL == self->conn || self->state == STATE_CONNECTING) {
        croak("not connected");
    }
    if (self->state != STATE_IDLE) {
        croak("another operation is in progress");
    }
    if (self->send_count > 0) {
        croak("cannot start operation while pipeline results are pending");
    }
    if (!(SvROK(cb) && SvTYPE(SvRV(cb)) == SVt_PVCV)) {
        croak("callback must be a CODE reference");
    }
    if (!SvROK(params_ref) || SvTYPE(SvRV(params_ref)) != SVt_PVAV) {
        croak("params must be an ARRAY reference");
    }

    stmt = INT2PTR(MYSQL_STMT *, stmt_iv);
    params = (AV *)SvRV(params_ref);
    nparams = (int)(av_len(params) + 1);

    if (nparams > 0) {
        Newxz(bind_params, nparams, MYSQL_BIND);

        for (i = 0; i < nparams; i++) {
            SV **svp = av_fetch(params, i, 0);
            if (svp && SvOK(*svp)) {
                STRLEN len;
                const char *s = SvPV(*svp, len);
                bind_params[i].buffer_type = MYSQL_TYPE_STRING;
                bind_params[i].buffer = (void *)s;
                bind_params[i].buffer_length = (unsigned long)len;
                bind_params[i].length = NULL;
                bind_params[i].is_null = NULL;
            } else {
                bind_params[i].buffer_type = MYSQL_TYPE_NULL;
            }
        }

        if (mysql_stmt_bind_param(stmt, bind_params)) {
            Safefree(bind_params);
            croak("mysql_stmt_bind_param failed: %s", mysql_stmt_error(stmt));
        }

        Safefree(bind_params);
    }

    push_cb(self, cb);
    self->op_stmt = stmt;

    self->state = STATE_STMT_EXECUTE;
    status = mysql_stmt_execute_start(&self->op_ret, stmt);
    if (status == 0) {
        if (self->op_ret != 0) {
            on_stmt_execute_done(self);
        } else {
            self->state = STATE_STMT_STORE;
            status = mysql_stmt_store_result_start(&self->op_ret, stmt);
            if (status == 0) {
                on_stmt_store_done(self);
            } else {
                update_watchers(self, status);
            }
        }
    } else {
        update_watchers(self, status);
    }
}

void
close_stmt(EV::MariaDB self, IV stmt_iv, SV *cb)
PREINIT:
    MYSQL_STMT *stmt;
    int status;
CODE:
{
    if (NULL == self->conn || self->state == STATE_CONNECTING) {
        croak("not connected");
    }
    if (self->state != STATE_IDLE) {
        croak("another operation is in progress");
    }
    if (self->send_count > 0) {
        croak("cannot start operation while pipeline results are pending");
    }
    if (!(SvROK(cb) && SvTYPE(SvRV(cb)) == SVt_PVCV)) {
        croak("callback must be a CODE reference");
    }

    stmt = INT2PTR(MYSQL_STMT *, stmt_iv);
    push_cb(self, cb);
    self->op_stmt = stmt;

    self->state = STATE_STMT_CLOSE;
    status = mysql_stmt_close_start(&self->op_bool_ret, stmt);
    if (status == 0) {
        on_stmt_close_done(self);
    } else {
        update_watchers(self, status);
    }
}

void
stmt_reset(EV::MariaDB self, IV stmt_iv, SV *cb)
PREINIT:
    MYSQL_STMT *stmt;
    int status;
CODE:
{
    if (NULL == self->conn || self->state == STATE_CONNECTING) {
        croak("not connected");
    }
    if (self->state != STATE_IDLE) {
        croak("another operation is in progress");
    }
    if (self->send_count > 0) {
        croak("cannot start operation while pipeline results are pending");
    }
    if (!(SvROK(cb) && SvTYPE(SvRV(cb)) == SVt_PVCV)) {
        croak("callback must be a CODE reference");
    }

    stmt = INT2PTR(MYSQL_STMT *, stmt_iv);
    push_cb(self, cb);
    self->op_stmt = stmt;

    self->state = STATE_STMT_RESET;
    status = mysql_stmt_reset_start(&self->op_bool_ret, stmt);
    if (status == 0) {
        on_stmt_reset_done(self);
    } else {
        update_watchers(self, status);
    }
}

void
ping(EV::MariaDB self, SV *cb)
PREINIT:
    int status;
CODE:
{
    if (NULL == self->conn || self->state == STATE_CONNECTING) {
        croak("not connected");
    }
    if (self->state != STATE_IDLE) {
        croak("another operation is in progress");
    }
    if (self->send_count > 0) {
        croak("cannot start operation while pipeline results are pending");
    }
    if (!(SvROK(cb) && SvTYPE(SvRV(cb)) == SVt_PVCV)) {
        croak("callback must be a CODE reference");
    }

    push_cb(self, cb);

    self->state = STATE_PING;
    status = mysql_ping_start(&self->op_ret, self->conn);
    if (status == 0) {
        on_ping_done(self);
    } else {
        update_watchers(self, status);
    }
}

void
change_user(EV::MariaDB self, const char *user, const char *password, SV *db_sv, SV *cb)
PREINIT:
    int status;
    const char *db;
CODE:
{
    if (NULL == self->conn || self->state == STATE_CONNECTING) {
        croak("not connected");
    }
    if (self->state != STATE_IDLE) {
        croak("another operation is in progress");
    }
    if (self->send_count > 0) {
        croak("cannot start operation while pipeline results are pending");
    }
    if (!(SvROK(cb) && SvTYPE(SvRV(cb)) == SVt_PVCV)) {
        croak("callback must be a CODE reference");
    }

    db = (SvOK(db_sv)) ? SvPV_nolen(db_sv) : NULL;
    push_cb(self, cb);

    self->state = STATE_CHANGE_USER;
    status = mysql_change_user_start(&self->op_bool_ret, self->conn, user, password, db);
    if (status == 0) {
        on_change_user_done(self);
    } else {
        update_watchers(self, status);
    }
}

void
select_db(EV::MariaDB self, const char *db, SV *cb)
PREINIT:
    int status;
CODE:
{
    if (NULL == self->conn || self->state == STATE_CONNECTING) {
        croak("not connected");
    }
    if (self->state != STATE_IDLE) {
        croak("another operation is in progress");
    }
    if (self->send_count > 0) {
        croak("cannot start operation while pipeline results are pending");
    }
    if (!(SvROK(cb) && SvTYPE(SvRV(cb)) == SVt_PVCV)) {
        croak("callback must be a CODE reference");
    }

    push_cb(self, cb);

    self->state = STATE_SELECT_DB;
    status = mysql_select_db_start(&self->op_ret, self->conn, db);
    if (status == 0) {
        on_select_db_done(self);
    } else {
        update_watchers(self, status);
    }
}

void
reset_connection(EV::MariaDB self, SV *cb)
PREINIT:
    int status;
CODE:
{
    if (NULL == self->conn || self->state == STATE_CONNECTING) {
        croak("not connected");
    }
    if (self->state != STATE_IDLE) {
        croak("another operation is in progress");
    }
    if (self->send_count > 0) {
        croak("cannot start operation while pipeline results are pending");
    }
    if (!(SvROK(cb) && SvTYPE(SvRV(cb)) == SVt_PVCV)) {
        croak("callback must be a CODE reference");
    }

    push_cb(self, cb);

    self->state = STATE_RESET_CONNECTION;
    status = mysql_reset_connection_start(&self->op_ret, self->conn);
    if (status == 0) {
        on_reset_connection_done(self);
    } else {
        update_watchers(self, status);
    }
}

void
_set_option(EV::MariaDB self, const char *key, SV *value)
CODE:
{
    if (strcmp(key, "connect_timeout") == 0) {
        self->connect_timeout = SvUV(value);
    } else if (strcmp(key, "read_timeout") == 0) {
        self->read_timeout = SvUV(value);
    } else if (strcmp(key, "write_timeout") == 0) {
        self->write_timeout = SvUV(value);
    } else if (strcmp(key, "compress") == 0) {
        self->compress = SvTRUE(value) ? 1 : 0;
    } else if (strcmp(key, "multi_statements") == 0) {
        self->multi_statements = SvTRUE(value) ? 1 : 0;
    } else if (strcmp(key, "charset") == 0) {
        if (self->charset) Safefree(self->charset);
        self->charset = safe_strdup(SvPV_nolen(value));
    } else if (strcmp(key, "init_command") == 0) {
        if (self->init_command) Safefree(self->init_command);
        self->init_command = safe_strdup(SvPV_nolen(value));
    } else if (strcmp(key, "ssl_key") == 0) {
        if (self->ssl_key) Safefree(self->ssl_key);
        self->ssl_key = safe_strdup(SvPV_nolen(value));
    } else if (strcmp(key, "ssl_cert") == 0) {
        if (self->ssl_cert) Safefree(self->ssl_cert);
        self->ssl_cert = safe_strdup(SvPV_nolen(value));
    } else if (strcmp(key, "ssl_ca") == 0) {
        if (self->ssl_ca) Safefree(self->ssl_ca);
        self->ssl_ca = safe_strdup(SvPV_nolen(value));
    } else if (strcmp(key, "ssl_cipher") == 0) {
        if (self->ssl_cipher) Safefree(self->ssl_cipher);
        self->ssl_cipher = safe_strdup(SvPV_nolen(value));
    } else if (strcmp(key, "ssl_verify_server_cert") == 0) {
        self->ssl_verify_server_cert = SvTRUE(value) ? 1 : 0;
    } else {
        croak("unknown option: %s", key);
    }
}

int
is_connected(EV::MariaDB self)
CODE:
{
    RETVAL = (NULL != self->conn && self->state != STATE_CONNECTING) ? 1 : 0;
}
OUTPUT:
    RETVAL

SV*
error_message(EV::MariaDB self)
CODE:
{
    if (NULL != self->conn) {
        const char *msg = mysql_error(self->conn);
        RETVAL = (msg && msg[0]) ? newSVpv(msg, 0) : &PL_sv_undef;
    }
    else {
        RETVAL = &PL_sv_undef;
    }
}
OUTPUT:
    RETVAL

unsigned int
error_number(EV::MariaDB self)
CODE:
{
    RETVAL = (NULL != self->conn) ? mysql_errno(self->conn) : 0;
}
OUTPUT:
    RETVAL

SV*
sqlstate(EV::MariaDB self)
CODE:
{
    if (NULL != self->conn) {
        const char *s = mysql_sqlstate(self->conn);
        RETVAL = (s && s[0]) ? newSVpv(s, 0) : &PL_sv_undef;
    }
    else {
        RETVAL = &PL_sv_undef;
    }
}
OUTPUT:
    RETVAL

SV*
insert_id(EV::MariaDB self)
CODE:
{
    if (NULL != self->conn) {
        my_ulonglong id = mysql_insert_id(self->conn);
        RETVAL = newSVuv((UV)id);
    }
    else {
        RETVAL = &PL_sv_undef;
    }
}
OUTPUT:
    RETVAL

unsigned int
warning_count(EV::MariaDB self)
CODE:
{
    RETVAL = (NULL != self->conn) ? mysql_warning_count(self->conn) : 0;
}
OUTPUT:
    RETVAL

SV*
info(EV::MariaDB self)
CODE:
{
    if (NULL != self->conn) {
        const char *i = mysql_info(self->conn);
        RETVAL = (i && i[0]) ? newSVpv(i, 0) : &PL_sv_undef;
    }
    else {
        RETVAL = &PL_sv_undef;
    }
}
OUTPUT:
    RETVAL

unsigned int
server_version(EV::MariaDB self)
CODE:
{
    RETVAL = (NULL != self->conn) ? (unsigned int)mysql_get_server_version(self->conn) : 0;
}
OUTPUT:
    RETVAL

SV*
server_info(EV::MariaDB self)
CODE:
{
    if (NULL != self->conn) {
        const char *info = mysql_get_server_info(self->conn);
        RETVAL = info ? newSVpv(info, 0) : &PL_sv_undef;
    }
    else {
        RETVAL = &PL_sv_undef;
    }
}
OUTPUT:
    RETVAL

unsigned int
thread_id(EV::MariaDB self)
CODE:
{
    RETVAL = (NULL != self->conn) ? (unsigned int)mysql_thread_id(self->conn) : 0;
}
OUTPUT:
    RETVAL

SV*
host_info(EV::MariaDB self)
CODE:
{
    if (NULL != self->conn) {
        const char *info = mysql_get_host_info(self->conn);
        RETVAL = info ? newSVpv(info, 0) : &PL_sv_undef;
    }
    else {
        RETVAL = &PL_sv_undef;
    }
}
OUTPUT:
    RETVAL

SV*
character_set_name(EV::MariaDB self)
CODE:
{
    if (NULL != self->conn) {
        const char *cs = mysql_character_set_name(self->conn);
        RETVAL = cs ? newSVpv(cs, 0) : &PL_sv_undef;
    }
    else {
        RETVAL = &PL_sv_undef;
    }
}
OUTPUT:
    RETVAL

int
socket(EV::MariaDB self)
CODE:
{
    RETVAL = (NULL != self->conn) ? (int)mysql_get_socket(self->conn) : -1;
}
OUTPUT:
    RETVAL

SV*
escape(EV::MariaDB self, SV *str)
PREINIT:
    STRLEN len;
    const char *s;
    char *escaped;
    unsigned long elen;
CODE:
{
    if (NULL == self->conn) {
        croak("not connected");
    }
    s = SvPV(str, len);
    Newx(escaped, len * 2 + 1, char);
    elen = mysql_real_escape_string(self->conn, escaped, s, (unsigned long)len);
    RETVAL = newSVpvn(escaped, elen);
    Safefree(escaped);
}
OUTPUT:
    RETVAL

int
pending_count(EV::MariaDB self)
CODE:
{
    RETVAL = self->pending_count;
}
OUTPUT:
    RETVAL

void
skip_pending(EV::MariaDB self)
CODE:
{
    if (self->state != STATE_IDLE || self->send_count > 0) {
        cleanup_connection(self);
    }
    cancel_pending(self, "skipped");
}

unsigned int
lib_version(char *class)
CODE:
{
    PERL_UNUSED_VAR(class);
    RETVAL = mysql_get_client_version();
}
OUTPUT:
    RETVAL

SV*
lib_info(char *class)
CODE:
{
    PERL_UNUSED_VAR(class);
    RETVAL = newSVpv(mysql_get_client_info(), 0);
}
OUTPUT:
    RETVAL
