import _ from './underscore-min';

(function (window, undefined) {
  window.MockEventGlobals = {
    setTimeout: 0,
    setInterval: 0,
    verbose: false,
    on: true,
  };

  var mockHandlers = [];
  var missed = [];

  var baseHandler = {
    id: null,
    url: '',
    setInterval: MockEventGlobals.setInterval,
    responses: [],
    response: null,
    proxy: null,
    on: MockEventGlobals.on,
    allResponses: [],

    initialize: function () {
      this.allResponses = this.allResponses.concat(this.responses);
    },
    headers: function () {
      return {
        'Access-Control-Allow-Credentials': true,
        'Access-Control-Allow-Headers': 'Content-type,Authorization',
        'Access-Control-Allow-Methods': 'GET,PUT,POST,DELETE,PATCH,OPTIONS',
        'Access-Control-Allow-Origin':
          window.location.protocol + '//' + window.location.host,
        'Access-Control-Expose-Headers': '*',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
        'Content-type': 'text/event-stream',
        Date: new Date().toString(),
      };
    },
    urlMatches: function (url) {
      if (_.isFunction(this.url.test)) {
        // The user provided a regex for the url, test it
        if (!this.url.test(url)) {
          return false;
        }
      } else {
        var star = this.url.indexOf('*');
        if (
          (this.url !== url && star === -1) ||
          !new RegExp(
            this.url
              .replace(/[-[\]{}()+?.,\\^$|#\s]/g, '\\$&')
              .replace(/\*/g, '.+'),
          ).test(url)
        ) {
          return false;
        }
      }
      return true;
    },
    clear: function () {
      mockHandlers[this.id] = null;
      delete this;
    },
    dispatchEvent: function (event) {
      return window.dispatchEvent(event);
    },
    send: function (response) {
      if (!(response.name && response.data)) {
        this.dispatchError(
          '`name` and `data` are required on mock handler response object',
        );
      } else {
        var evt = new Event(response.name);
        evt.data = JSON.stringify(response.data);
        this.dispatchEvent(evt);
      }
    },
    errorEventName: function () {
      return 'mock-event-' + this.id + '-error';
    },
    dispatchError: function (errorMessage) {
      var evt = new Event(this.errorEventName());
      evt.error = errorMessage;
      this.dispatchEvent(evt);
    },
    stream: function (responses) {
      /* Handling the stream output via this.setInterval attribute, 
            ironically it's being handled with the `setTimeout` function. */
      var self = this;

      var streamIt = function () {
        if (responses.length) {
          var response = responses.shift();
          if (self.evtSource.readyState === self.evtSource.OPEN) {
            self.lastResponseId = response.id;
            self.send(response);
            self.stream(responses);
          } else {
            if (MockEventGlobals.verbose)
              console.warn(
                'Missed response because EventSource.close()',
                response,
              );
            self.dispatchError('`EventSource` instance closed while sending.');
          }
        } else {
          clearTimeout(timeoutId);
          timeoutId = false;
        }
      };

      if (!timeoutId) {
        if (self.setInterval instanceof Array) {
          var min = self.setInterval[0];
          var max = self.setInterval[1];
          var timeoutValue = Math.random() * (max - min) + min;
          var timeoutId = setTimeout(streamIt, timeoutValue);
        } else {
          var timeoutValue = self.setInterval;
          var timeoutId = setTimeout(streamIt, timeoutValue);
        }

        // Logging on `verbose` = True
        if (MockEventGlobals.verbose && responses.length) {
          console.info('Send stream in ' + timeoutValue + ' milliseconds.');
        }
      }
    },

    stop: function () {
      this.on = false;
    },
    start: function () {
      this.on = true;
    },
  };

  window.EventSource = function (url, settings) {
    var self = this;

    self.url = url;
    self.settings = settings;

    self.CONNECTING = 0;
    self.OPEN = 1;
    self.CLOSED = 2;

    self.readyState = null;

    self.headers = function () {
      return {
        Accept: 'text/event-stream',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Accept-Language': window.navigator.languages.join(','),
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
        // 'Cookie': docCookies.cookiesToString(),
        Host: window.location.host,
        // 'Last-event-id': this.handler.lastResponseId || '',
        Origin: window.location.protocol + '//' + window.location.host,
        Referer: window.location.protocol + '//' + window.location.host,
        'User-Agent': window.navigator.userAgent,
      };
    };

    // Creates event of type `error`
    self.error = function (message) {
      var evt = new Event('error');
      evt.error = message;
      if (self.onerror) {
        self.onerror(evt);
      }
    };

    self.close = function () {
      self.readyState = self.CLOSED;
    };

    self.addEventListener = function (name, fn) {
      return window.addEventListener(name, fn, false);
    };

    self.removeEventListener = function (name, fn) {
      return window.removeEventListener(name, fn, false);
    };

    self.listenForErrors = function (mockHandler) {
      self.addEventListener(mockHandler.errorEventName(), function (event) {
        self.error(event.error);
      });
    };

    self.responses = [];

    // Properties are set in several statements
    // waiting for all properties to be set
    setTimeout(function () {
      // No `MockEvent` instances detected
      if (mockHandlers.length === 0) missed.push(self);

      _.each(mockHandlers, function (mockHandler) {
        // `MockEvent` instance deleted
        if (mockHandler === null) return;

        if (!mockHandler.urlMatches(self.url)) {
          missed.push(self);
          return;
        }

        self.handler = mockHandler;
        mockHandler.evtSource = self;

        // mockHandler dispatches error event
        // EventSource calls `onerror` method
        self.listenForErrors(mockHandler);

        if (self.readyState === null) {
          self.readyState = self.CONNECTING;
        }

        if (self.readyState == self.CONNECTING) {
          if (self.onopen) {
            self.onopen({
              message: "You're open!",
              apology: "I didn't know what else to say.",
            });
          }
          self.readyState = self.OPEN;
        }

        if (!(mockHandler.allResponses.length || mockHandler.response)) {
          self.error(
            'Handler ' + mockHandler.url + ' requires response type attribute',
          );
        }

        if (mockHandler.response) {
          mockHandler.response(mockHandler, self);
        }

        if (mockHandler.responses) {
          mockHandler.stream(_.clone(mockHandler.responses));
        }
      });

      if (self.handler === undefined) {
        /* A handler was never found for this `EventSource`
                instance. In this case we send a Timeout Error. */
        self.error('Timeout Error');
      }
    }, MockEventGlobals.setTimeout);
  };
  window.MockEvent = function (settings) {
    var i = mockHandlers.length;
    mockHandlers[i] = _.extend({}, baseHandler, settings, { id: i });
    mockHandlers[i].initialize();
    return mockHandlers[i];
  };

  MockEvent.clear = function (i) {
    if (i || i === 0) {
      mockHandlers[i] = null;
    } else {
      mockHandlers = [];
    }
  };

  MockEvent.handlers = function (i) {
    if (i || i === 0) {
      return mockHandlers[i];
    } else {
      return mockHandlers;
    }
  };

  MockEvent.missed = function () {
    return missed;
  };
})(window);
