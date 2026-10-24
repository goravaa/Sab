const $ = (id) => document.getElementById(id);
const sourceType = $("source-type");
const checkButton = $("check-source");
const previewButton = $("preview");
const submitButton = $("submit-job");

function setBusy(busy) {
  checkButton.disabled = busy;
  previewButton.disabled = busy;
  submitButton.disabled = busy;
}

function showError(message) {
  const target = $("error-message");
  target.textContent = message;
  target.hidden = !message;
}

function optional(value) {
  const trimmed = value.trim();
  return trimmed || undefined;
}

function buildConnection() {
  if (sourceType.value === "postgres") {
    const tables = $("db-tables").value.split(",").map((name) => name.trim()).filter(Boolean);
    const connection = {
      type: "postgres",
      host: $("db-host").value.trim(),
      port: Number($("db-port").value),
      database: $("db-name").value.trim(),
      username: $("db-user").value.trim(),
      password: $("db-password").value,
    };
    if (tables.length) connection.tables = tables;
    return connection;
  }
  const connection = {
    type: "s3",
    bucket: $("source-bucket").value.trim(),
    access_key_id: $("source-access").value.trim(),
    secret_access_key: $("source-secret").value,
    prefix: $("source-prefix").value.trim(),
  };
  const region = optional($("source-region").value);
  const endpoint = optional($("source-endpoint").value);
  if (region) connection.region = region;
  if (endpoint) connection.endpoint_url = endpoint;
  return connection;
}

function buildTarget() {
  const target = {
    type: "s3",
    bucket: $("target-bucket").value.trim(),
    access_key_id: $("target-access").value.trim(),
    secret_access_key: $("target-secret").value,
    prefix: $("target-prefix").value.trim(),
  };
  const region = optional($("target-region").value);
  const endpoint = optional($("target-endpoint").value);
  if (region) target.region = region;
  if (endpoint) target.endpoint_url = endpoint;
  return target;
}

function buildBody() {
  const name = $("source-name").value.trim();
  return {
    dataset: $("dataset").value.trim(),
    sources: [{ name, connection: buildConnection(), options: {} }],
    target: buildTarget(),
  };
}

function setSourceType() {
  const postgres = sourceType.value === "postgres";
  $("postgres-fields").hidden = !postgres;
  $("s3-fields").hidden = postgres;
  $("db-host").required = postgres;
  $("db-port").required = postgres;
  $("db-name").required = postgres;
  $("db-user").required = postgres;
  $("db-password").required = postgres;
  $("source-bucket").required = !postgres;
  $("source-access").required = !postgres;
  $("source-secret").required = !postgres;
  $("source-result").textContent = "";
}

async function postJson(url, body) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.detail || `Request failed (${response.status})`);
  return data;
}

function renderDiff(diff) {
  $("diff-new").textContent = diff.new ?? 0;
  $("diff-modified").textContent = diff.modified ?? 0;
  $("diff-deleted").textContent = diff.deleted ?? 0;
}

checkButton.addEventListener("click", async () => {
  showError("");
  const result = $("source-result");
  result.className = "inline-result";
  result.textContent = "Checking…";
  setBusy(true);
  try {
    const data = await postJson("/v1/sources/check", {
      name: $("source-name").value.trim(),
      config: buildConnection(),
    });
    const info = data.info || {};
    const summary = info.row_count !== undefined
      ? `${info.row_count.toLocaleString()} rows · ${(info.tables || []).length} tables`
      : `${(info.object_count || 0).toLocaleString()} objects · ${formatBytes(info.size_bytes || 0)}`;
    result.className = "inline-result success";
    result.textContent = `Connected · ${summary}`;
  } catch (error) {
    result.className = "inline-result failure";
    result.textContent = error.message;
  } finally {
    setBusy(false);
  }
});

previewButton.addEventListener("click", async () => {
  showError("");
  if (!$("setup-form").reportValidity()) return;
  setBusy(true);
  try {
    const diff = await postJson("/v1/datasets/reconcile", buildBody());
    renderDiff(diff);
    $("result-title").textContent = "Changes detected";
    $("result-badge").textContent = "Preview";
    $("result-badge").className = "badge";
    $("progress-area").hidden = true;
    $("result-panel").hidden = false;
    $("result-panel").scrollIntoView({ behavior: "smooth", block: "nearest" });
  } catch (error) {
    showError(error.message);
  } finally {
    setBusy(false);
  }
});

submitButton.addEventListener("click", async () => {
  showError("");
  const form = $("setup-form");
  if (!form.reportValidity()) return;
  setBusy(true);
  $("result-panel").hidden = false;
  $("progress-area").hidden = false;
  $("result-title").textContent = "Indexing dataset";
  $("result-badge").textContent = "Running";
  $("result-badge").className = "badge running";
  $("progress-bar").value = 0;
  $("progress-value").textContent = "0%";
  $("progress-label").textContent = "Preparing job…";
  $("job-message").textContent = "Scanning source and preparing batches.";
  $("result-panel").scrollIntoView({ behavior: "smooth", block: "nearest" });

  try {
    const response = await fetch("/v1/jobs/stream", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(buildBody()),
    });
    if (!response.ok) {
      const error = await response.json().catch(() => ({}));
      throw new Error(error.detail || `Request failed (${response.status})`);
    }
    if (!response.body) throw new Error("This browser does not support streamed responses.");
    await readEvents(response.body);
  } catch (error) {
    $("result-title").textContent = "Could not start indexing";
    $("result-badge").textContent = "Failed";
    $("result-badge").className = "badge failed";
    $("job-message").textContent = error.message;
    showError(error.message);
  } finally {
    setBusy(false);
  }
});

async function readEvents(stream) {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  while (true) {
    const { value, done } = await reader.read();
    buffer += decoder.decode(value || new Uint8Array(), { stream: !done });
    const events = buffer.split(/\r?\n\r?\n/);
    buffer = events.pop() || "";
    for (const block of events) handleEvent(block);
    if (done) break;
  }
  if (buffer.trim()) handleEvent(buffer);
}

function handleEvent(block) {
  let eventName = "message";
  const dataLines = [];
  for (const line of block.split(/\r?\n/)) {
    if (line.startsWith("event:")) eventName = line.slice(6).trim();
    if (line.startsWith("data:")) dataLines.push(line.slice(5).trim());
  }
  if (!dataLines.length) return;
  const data = JSON.parse(dataLines.join("\n"));
  if (eventName === "job") {
    renderDiff(data.diff || {});
    $("job-message").textContent = `Job ${data.id} accepted.`;
    return;
  }
  if (eventName === "progress") {
    const progress = data.progress || {};
    const ratio = Math.min(1, Math.max(0, Number(progress.ratio || 0)));
    $("progress-bar").value = ratio;
    $("progress-value").textContent = `${Math.round(ratio * 100)}%`;
    $("progress-label").textContent = `${(progress.processed_items || 0).toLocaleString()} of ${(progress.total_items || 0).toLocaleString()} items`;
    $("job-message").textContent = `${progress.completed_batches || 0} batches completed.`;
    return;
  }
  if (eventName === "completed") {
    renderDiff(data.diff || {});
    $("result-title").textContent = "Indexing complete";
    $("result-badge").textContent = "Complete";
    $("result-badge").className = "badge done";
    const progress = data.progress || {};
    $("progress-bar").value = 1;
    $("progress-value").textContent = "100%";
    $("progress-label").textContent = `${(progress.processed_items || 0).toLocaleString()} of ${(progress.total_items || 0).toLocaleString()} items`;
    $("job-message").textContent = "Your dataset is ready to query.";
    return;
  }
  if (eventName === "failed") {
    $("result-title").textContent = "Indexing failed";
    $("result-badge").textContent = "Failed";
    $("result-badge").className = "badge failed";
    $("job-message").textContent = data.error || "The job failed.";
    showError(data.error || "The indexing job failed.");
  }
}

function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let size = bytes / 1024;
  let unit = units[0];
  for (let index = 1; size >= 1024 && index < units.length; index += 1) {
    size /= 1024;
    unit = units[index];
  }
  return `${size.toFixed(1)} ${unit}`;
}

sourceType.addEventListener("change", setSourceType);
setSourceType();
