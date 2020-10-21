import { fetchWrapper } from '../fetchWrapper';

export const versionUrl = '/v1/public/version';

export type GetVersionResponse = {
  productName: string;
  version: string;
};
export async function getVersion(): Promise<GetVersionResponse> {
  const response = await fetchWrapper.get(versionUrl);
  return response.json();
}
