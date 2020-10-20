import { rest } from 'msw';
import {
  GetVisibleContextsResponse,
  visibleContextsUrl,
} from './visibleContexts';

const getVisibleContextsResponseMock: GetVisibleContextsResponse = [
  'default',
  'billing',
];

export const mockGetVisibleContexts = rest.get(
  visibleContextsUrl,
  (req, res, ctx) => {
    const query = req.url.searchParams;
    const includeAdmin = query.get('includeAdmin');

    // Add case when `includeAdmin=true`
    return res(ctx.json(getVisibleContextsResponseMock));
  },
);
