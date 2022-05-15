export const extractFileName = (_url: string) =>
    new URL(decodeURIComponent(_url)).pathname?.split('/').pop();
