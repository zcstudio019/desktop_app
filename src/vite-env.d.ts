/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_API_BASE: string;
  readonly VITE_DIRECT_JOB_API_BASE: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
