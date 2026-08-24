// 轻量可变状态容器，统一 get/set 入口，减少散落的 let 全局变量。
export function createMutableState(initialState = {}) {
  const state = { ...initialState };

  function get(key) {
    return state[key];
  }

  function set(key, value) {
    state[key] = value;
    return value;
  }

  function patch(nextState) {
    // 批量更新用于跨模块同步多字段，避免多次 set 的中间态闪烁。
    Object.assign(state, nextState || {});
    return state;
  }

  function pick(keys) {
    const out = {};
    for (let i = 0; i < keys.length; i += 1) {
      out[keys[i]] = state[keys[i]];
    }
    return out;
  }

  function all() {
    // 直接暴露内部对象，调用方应只用于调试场景读取。
    return state;
  }

  return {
    get,
    set,
    patch,
    pick,
    all,
  };
}
