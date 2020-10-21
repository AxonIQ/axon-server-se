const defaultHeaders = new Headers();
defaultHeaders.append('Content-Type', 'application/json');

const defaultOptions: RequestInit = {
  cache: 'no-cache',
  credentials: 'same-origin',
  headers: defaultHeaders,
};

type ResponseInterceptor = (response: Response) => Response;

async function handleRequest(
  url: string,
  options: RequestInit,
  responseInterceptor: ResponseInterceptor,
) {
  let rootUrl = '';
  if (import.meta.env && import.meta.env.MODE === 'development') {
    rootUrl = 'https://localhost:8024';
  }
  const response = await fetch(`${rootUrl}${url}`, options);
  responseInterceptor(response);

  if (!response.ok) {
    const contentType = response.headers.get('content-type');
    if (contentType && contentType.indexOf('application/json') !== -1) {
      const errorObject = await response.json();
      errorObject.statusCode = response.status;
      throw errorObject;
    } else {
      const error = {
        message: await response.text(),
        statusCode: response.status,
      };
      throw error;
    }
  }

  return response;
}

export const fetchWrapper = {
  responseInterceptor(response: Response) {
    return response;
  },

  setAuthorizationToken(authToken: string) {
    // Clean out any authorization headers, before setting this one
    this.removeAuthorizationToken();
    defaultHeaders.append('Authorization', `Bearer ${authToken}`);
  },
  removeAuthorizationToken() {
    defaultHeaders.delete('Authorization');
  },

  async get(url: string, customOptions?: RequestInit) {
    const options = {
      ...defaultOptions,
      method: 'GET',
      ...customOptions,
    };
    return handleRequest(url, options, this.responseInterceptor);
  },
  async post(url: string, body?: any, customOptions?: RequestInit) {
    const options: RequestInit = {
      ...defaultOptions,
      method: 'POST',
      body: JSON.stringify(body),
      ...customOptions,
    };
    return handleRequest(url, options, this.responseInterceptor);
  },

  async put(url: string, body?: any, customOptions?: RequestInit) {
    const options = {
      ...defaultOptions,
      method: 'PUT',
      body: JSON.stringify(body),
      ...customOptions,
    };
    return handleRequest(url, options, this.responseInterceptor);
  },

  async patch(url: string, body?: any, customOptions?: RequestInit) {
    const options = {
      ...defaultOptions,
      method: 'PATCH',
      body: JSON.stringify(body),
      ...customOptions,
    };
    return handleRequest(url, options, this.responseInterceptor);
  },

  // We use this because `delete` is a reserved word.
  async remove(url: string, customOptions?: RequestInit) {
    const options = {
      ...defaultOptions,
      method: 'DELETE',
      ...customOptions,
    };
    return handleRequest(url, options, this.responseInterceptor);
  },
};
