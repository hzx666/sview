// 评分通用控件：布尔互斥选择与支持半分的五星评分。

export function createBooleanChoice(root, initialValue = null) {
  if (!(root instanceof HTMLElement)) {
    throw new Error("评分布尔选择控件未挂载");
  }
  let value = initialValue === null ? null : Boolean(initialValue);
  const buttons = Array.from(root.querySelectorAll("button[data-value]"));
  if (buttons.length !== 2) {
    throw new Error("评分布尔选择控件必须包含是、否两个选项");
  }

  function render() {
    for (const button of buttons) {
      const selected = value !== null && (button.dataset.value === "true") === value;
      button.classList.toggle("selected", selected);
      button.setAttribute("aria-pressed", String(selected));
    }
  }

  for (const button of buttons) {
    button.addEventListener("click", () => {
      value = button.dataset.value === "true";
      render();
    });
  }
  render();
  return {
    getValue: () => value,
    reset(nextValue = null) {
      value = nextValue === null ? null : Boolean(nextValue);
      render();
    },
  };
}

export function createStarRating(root, valueLabel, zeroButton) {
  if (!(root instanceof HTMLElement) || !(valueLabel instanceof HTMLElement) || !(zeroButton instanceof HTMLElement)) {
    throw new Error("星级评分控件未挂载");
  }
  let value = null;

  function render(displayValue = value) {
    const hasValue = displayValue !== null;
    const normalized = hasValue ? Number(displayValue) : 0;
    root.style.setProperty("--score-fill", `${(normalized / 5) * 100}%`);
    root.setAttribute("aria-valuenow", String(normalized));
    valueLabel.textContent = hasValue ? `${normalized.toFixed(1)} 分` : "未评分";
    zeroButton.classList.toggle("selected", hasValue && normalized === 0);
  }

  function valueFromPointer(event) {
    const rect = root.getBoundingClientRect();
    const ratio = Math.min(1, Math.max(0, (event.clientX - rect.left) / rect.width));
    return Math.max(0.5, Math.ceil(ratio * 10) / 2);
  }

  root.addEventListener("pointermove", (event) => render(valueFromPointer(event)));
  root.addEventListener("pointerleave", () => render());
  root.addEventListener("pointerdown", (event) => {
    value = valueFromPointer(event);
    render();
  });
  zeroButton.addEventListener("click", () => {
    value = 0;
    render();
  });
  root.addEventListener("keydown", (event) => {
    if (!["ArrowLeft", "ArrowDown", "ArrowRight", "ArrowUp"].includes(event.key)) return;
    event.preventDefault();
    const direction = ["ArrowRight", "ArrowUp"].includes(event.key) ? 0.5 : -0.5;
    value = Math.min(5, Math.max(0, (value ?? 0) + direction));
    render();
  });
  render();
  return {
    getValue: () => value,
    reset(nextValue = null) {
      value = nextValue === null ? null : Number(nextValue);
      render();
    },
  };
}
