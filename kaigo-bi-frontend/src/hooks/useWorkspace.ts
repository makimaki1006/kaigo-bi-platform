"use client";

// ===================================================
// ワークスペース（ペルソナ）切替フック
// 選択状態をlocalStorageに永続化する
// ===================================================

import { useCallback, useState } from "react";
import {
  WORKSPACES,
  WORKSPACE_STORAGE_KEY,
  type Workspace,
  type WorkspaceId,
} from "@/lib/constants";

function readStoredWorkspace(): WorkspaceId {
  if (typeof window === "undefined") return "all";
  const stored = window.localStorage.getItem(WORKSPACE_STORAGE_KEY);
  if (stored && WORKSPACES.some((w) => w.id === stored)) {
    return stored as WorkspaceId;
  }
  return "all";
}

export function useWorkspace() {
  // React 19: useEffect内のsetStateはコミットされないことがあるため初期化関数で読む
  const [workspaceId, setWorkspaceId] = useState<WorkspaceId>(readStoredWorkspace);

  const setWorkspace = useCallback((id: WorkspaceId) => {
    setWorkspaceId(id);
    try {
      window.localStorage.setItem(WORKSPACE_STORAGE_KEY, id);
    } catch {
      // プライベートモード等で保存できなくても動作は継続
    }
  }, []);

  const workspace: Workspace =
    WORKSPACES.find((w) => w.id === workspaceId) ?? WORKSPACES[0];

  return { workspace, workspaceId, setWorkspace };
}

/** サインアップ時など、フック外からワークスペースを保存する */
export function persistWorkspace(id: WorkspaceId): void {
  try {
    window.localStorage.setItem(WORKSPACE_STORAGE_KEY, id);
  } catch {
    // 保存失敗は無視
  }
}
