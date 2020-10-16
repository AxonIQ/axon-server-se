export const versionUrl = '/v1/public/version';

export type GetVersionResponse = {
  productName: string;
  version: string;
};
export async function getVersion(): Promise<GetVersionResponse> {
  const response = await fetch(versionUrl);
  return response.json();
}
