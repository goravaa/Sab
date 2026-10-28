const $ = (id) => document.getElementById(id);
const reloadButton = $("reload-workspace");
const previewButton = $("preview");
const submitButton = $("submit-job");
const ACTIVE_JOB_KEY = "sab-dataset-engine:last-job";

let workspace = null;
let progressSamples = [];
let activeJobRunning = false;
let jobPollTimer = null;

function setBusy(busy) {
  busy = busy || activeJobRunning;
  reloadButton.disabled = busy;
  previewButton.disabled = busy || !workspace;
  submitButton.disabled = busy || !workspace;
}

function showError(message) {
  const target = $("error-message");
  target.textContent = message;
  target.hidden = !message;
}

function formatNumber(value) {
  return Number(value || 0).toLocaleString();
}

function formatDuration(seconds) {
  if (!Number.isFinite(seconds) || seconds < 0) return "Estimating time remaining…";
  const remaining = Math.ceil(seconds);
  if (remaining < 60) return "Less than a minute remaining";
  const minutes = Math.ceil(remaining / 60);
  if (minutes < 60) return `About ${minutes} min remaining`;
  const hours = Math.floor(minutes / 60);
  const extraMinutes = minutes % 60;
  if (hours < 24) {
    return extraMinutes
      ? `About ${hours} hr ${extraMinutes} min remaining`
      : `About ${hours} hr remaining`;
  }
  const days = Math.floor(hours / 24);
  const extraHours = hours % 24;
  return extraHours
    ? `About ${days} d ${extraHours} hr remaining`
    : `About ${days} d remaining`;
}

function updateProgressEta(progress) {
  const now = performance.now();
  const processed = Number(progress.processed_items || 0);
  progressSamples.push({ processed, at: now });
  if (progressSamples.length > 5) progressSamples.shift();

  const first = progressSamples[0];
  const last = progressSamples[progressSamples.length - 1];
  const deltaItems = last.processed - first.processed;
  const elapsedSeconds = (last.at - first.at) / 1000;
  const remainingItems = Math.max(0, Number(progress.total_items || 0) - processed);

  if (remainingItems === 0) {
    $("progress-eta").textContent = "Finishing up…";
  } else if (deltaItems <= 0 || elapsedSeconds <= 0) {
    $("progress-eta").textContent = "Estimating time remaining…";
  } else {
    const itemsPerSecond = deltaItems / elapsedSeconds;
    $("progress-eta").textContent = formatDuration(remainingItems / itemsPerSecond);
  }
}

function authLabel(mode) {
  if (mode === "password") return "Password";
  if (mode === "static") return "Access keys";
  return "IAM chain";
}

function sourceAccess(access) {
  if (!access || access.reachable === null) {
    return { label: "Not checked", className: "pending" };
  }
  if (access.reachable) {
    return { label: "Accessible", className: "success" };
  }
  return { label: "Unavailable", className: "failure" };
}

async function getJson(url) {
  const response = await fetch(url, { headers: { Accept: "application/json" } });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.detail || `Request failed (${response.status})`);
  return data;
}

function saveJobReference(job) {
  if (!job?.id || !workspace) return;
  localStorage.setItem(ACTIVE_JOB_KEY, JSON.stringify({
    id: job.id,
    dataset: workspace.dataset,
    target_uri: workspace.target?.uri,
  }));
}

function storedJobReference() {
  try {
    return JSON.parse(localStorage.getItem(ACTIVE_JOB_KEY) || "null");
  } catch {
    localStorage.removeItem(ACTIVE_JOB_KEY);
    return null;
  }
}

function showJobProgress(job, { restored = false } = {}) {
  const status = job.status || "running";
  const progress = job.progress || {};
  const running = ["preparing", "pending", "running"].includes(status);
  activeJobRunning = running;
  $("progress-area").hidden = false;
  renderDiff(job.diff || {});
  $("result-title").textContent = running ? "Indexing dataset" : status === "completed" ? "Indexing complete" : "Indexing failed";
  $("result-badge").textContent = running ? status === "preparing" ? "Preparing" : "Running" : status === "completed" ? "Complete" : "Failed";
  $("result-badge").className = `badge ${running ? "running" : status === "completed" ? "done" : "failed"}`;
  const ratio = Math.min(1, Math.max(0, Number(progress.ratio || 0)));
  $("progress-bar").value = ratio;
  $("progress-value").textContent = `${Math.round(ratio * 100)}%`;
  $("progress-label").textContent = `${formatNumber(progress.processed_items)} of ${formatNumber(progress.total_items)} items`;

  if (status === "preparing") {
    $("job-message").textContent = "Scanning configured sources and preparing batches.";
    $("progress-eta").textContent = "ETA starts after the source scan.";
  } else if (running) {
    $("job-message").textContent = `${formatNumber(progress.completed_batches)} batches completed.${restored ? " Restored after page reload." : ""}`;
    updateProgressEta(progress);
  } else if (status === "completed") {
    $("progress-bar").value = 1;
    $("progress-value").textContent = "100%";
    $("job-message").textContent = "The dataset is ready to query.";
    $("progress-eta").textContent = "Complete";
  } else {
    $("job-message").textContent = job.error || "The job failed.";
    $("progress-eta").textContent = "ETA unavailable because indexing failed.";
  }
  setBusy(false);
  if (status === "failed") showError(job.error || "The indexing job failed.");
}

function scheduleJobPoll(callback) {
  if (jobPollTimer) clearTimeout(jobPollTimer);
  jobPollTimer = setTimeout(callback, 1500);
}

async function pollStoredJob(jobId) {
  if (!workspace || !activeJobRunning) return;
  try {
    const job = await getJson(`/v1/workspace/jobs/${encodeURIComponent(jobId)}`);
    saveJobReference(job);
    showJobProgress(job, { restored: true });
    if (activeJobRunning) scheduleJobPoll(() => pollStoredJob(jobId));
  } catch (error) {
    showError(error.message);
    activeJobRunning = false;
    setBusy(false);
  }
}

async function restoreJobAfterReload() {
  if (!workspace) return;
  progressSamples = [];
  try {
    const activeResponse = await getJson("/v1/workspace/jobs/active");
    const active = activeResponse.job;
    if (active) {
      if (active.id) saveJobReference(active);
      showJobProgress(active, { restored: true });
      if (active.id) {
        scheduleJobPoll(() => pollStoredJob(active.id));
      } else if (activeJobRunning) {
        scheduleJobPoll(restoreJobAfterReload);
      }
      return;
    }
  } catch (error) {
    // A previously completed job can still be restored from its saved ID.
    console.warn("Could not check for an active job", error);
  }

  const saved = storedJobReference();
  if (!saved || saved.dataset !== workspace.dataset || saved.target_uri !== workspace.target?.uri) return;
  try {
    const job = await getJson(`/v1/workspace/jobs/${encodeURIComponent(saved.id)}`);
    showJobProgress(job, { restored: true });
    if (activeJobRunning) scheduleJobPoll(() => pollStoredJob(saved.id));
  } catch {
    localStorage.removeItem(ACTIVE_JOB_KEY);
  }
}

async function postJson(url) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.detail || `Request failed (${response.status})`);
  return data;
}

function resetWorkspaceView(message) {
  workspace = null;
  $("dataset-name").textContent = "—";
  $("target-short").textContent = "No target loaded";
  $("workspace-summary").textContent = message;
  $("api-state").textContent = "Config error";
  $("source-count").textContent = "0 sources";
  $("source-list").replaceChildren(sourceEmpty(message));
  $("target-dataset").textContent = "—";
  $("target-uri").textContent = message;
  $("target-auth").textContent = "Auth —";
  $("target-state").textContent = "Waiting for workspace config";
  $("target-state").className = "inline-result";
  $("indexed-documents").textContent = "0";
  $("indexed-items").textContent = "0";
  $("orphaned-documents").textContent = "0";
  setBusy(false);
}

function sourceEmpty(message) {
  const item = document.createElement("li");
  item.className = "source-empty";
  item.textContent = message;
  return item;
}

function sourceCard(source) {
  const access = sourceAccess(source.access);
  const item = document.createElement("li");
  item.className = "source-card";

  const head = document.createElement("div");
  head.className = "source-card-head";

  const identity = document.createElement("div");
  identity.className = "source-identity";

  const type = document.createElement("span");
  type.className = "source-type";
  type.textContent = source.type === "postgres" ? "DB" : "S3";

  const names = document.createElement("div");
  names.className = "source-names";
  const label = document.createElement("strong");
  label.textContent = source.label;
  const name = document.createElement("span");
  name.textContent = source.name;
  names.append(label, name);

  identity.append(type, names);

  const status = document.createElement("span");
  status.className = `status-pill ${access.className}`;
  status.textContent = access.label;
  if (source.access?.error) status.title = source.access.error;

  head.append(identity, status);

  const description = document.createElement("p");
  description.className = "source-description";
  description.textContent = source.description || "No description in workspace config.";

  const location = document.createElement("code");
  location.className = "source-location";
  location.textContent = source.location;

  const stats = document.createElement("div");
  stats.className = "source-stats";
  const docs = document.createElement("span");
  docs.textContent = `${formatNumber(source.indexed_documents)} docs indexed`;
  const items = document.createElement("span");
  items.textContent = `${formatNumber(source.indexed_items)} source ${source.indexed_items === 1 ? "item" : "items"}`;
  const auth = document.createElement("span");
  auth.textContent = authLabel(source.auth);
  stats.append(docs, items, auth);

  item.append(head, description, location, stats);
  return item;
}

function renderWorkspace(data) {
  workspace = data;
  const target = data.target || {};
  const status = target.status || {};
  const sources = data.sources || [];

  $("dataset-name").textContent = data.dataset || "—";
  $("target-short").textContent = target.uri || "No target";
  $("workspace-summary").textContent = `${sources.length} configured ${sources.length === 1 ? "source" : "sources"} → ${target.uri || "no target"}`;
  $("source-count").textContent = `${sources.length} ${sources.length === 1 ? "source" : "sources"}`;
  $("target-dataset").textContent = data.dataset || "—";
  $("target-uri").textContent = target.uri || "No target";
  $("target-auth").textContent = authLabel(target.auth);
  $("indexed-documents").textContent = formatNumber(status.indexed_documents);
  $("indexed-items").textContent = formatNumber(status.indexed_items);
  $("orphaned-documents").textContent = formatNumber(status.orphaned_documents);

  if (status.error) {
    $("target-state").textContent = "Target unavailable";
    $("target-state").className = "inline-result failure";
    $("target-state").title = status.error;
    $("api-state").textContent = "Target unavailable";
  } else if (!status.config_present) {
    $("target-state").textContent = "Not indexed yet";
    $("target-state").className = "inline-result pending";
    $("api-state").textContent = "Workspace loaded";
  } else if (status.definition_changed) {
    $("target-state").textContent = "Definition changed";
    $("target-state").className = "inline-result warning";
    $("api-state").textContent = "Workspace loaded";
  } else {
    $("target-state").textContent = "Definition current";
    $("target-state").className = "inline-result success";
    $("api-state").textContent = "Workspace loaded";
  }

  const list = $("source-list");
  list.replaceChildren();
  if (!sources.length) {
    list.append(sourceEmpty("No sources configured."));
  } else {
    for (const source of sources) list.append(sourceCard(source));
  }

  setBusy(false);
}

async function loadWorkspace({ quiet = false } = {}) {
  if (!quiet) {
    setBusy(true);
    $("api-state").textContent = "Checking workspace";
  }
  showError("");
  try {
    const data = await getJson("/v1/workspace/status");
    renderWorkspace(data);
    await restoreJobAfterReload();
  } catch (error) {
    resetWorkspaceView(error.message);
    showError(error.message);
  }
}

function renderDiff(diff) {
  $("diff-new").textContent = formatNumber(diff.new);
  $("diff-modified").textContent = formatNumber(diff.modified);
  $("diff-deleted").textContent = formatNumber(diff.deleted);
}

reloadButton.addEventListener("click", () => loadWorkspace());

previewButton.addEventListener("click", async () => {
  if (!workspace) return;
  showError("");
  setBusy(true);
  try {
    const diff = await postJson("/v1/workspace/reconcile");
    renderDiff(diff);
    $("result-title").textContent = "Changes detected";
    $("result-badge").textContent = "Preview";
    $("result-badge").className = "badge";
    $("progress-area").hidden = true;
  } catch (error) {
    showError(error.message);
  } finally {
    setBusy(false);
  }
});

submitButton.addEventListener("click", async () => {
  if (!workspace) return;
  showError("");
  setBusy(true);
  $("progress-area").hidden = false;
  $("result-title").textContent = "Indexing dataset";
  $("result-badge").textContent = "Running";
  $("result-badge").className = "badge running";
  $("progress-bar").value = 0;
  $("progress-value").textContent = "0%";
  $("progress-label").textContent = "Preparing job…";
  $("job-message").textContent = "Scanning configured sources and preparing batches.";
  $("progress-eta").textContent = "ETA starts after the source scan.";
  progressSamples = [];

  try {
    const response = await fetch("/v1/workspace/jobs/stream", { method: "POST" });
    if (!response.ok) {
      const error = await response.json().catch(() => ({}));
      if (response.status === 409) {
        await restoreJobAfterReload();
        if (!activeJobRunning) {
          $("result-title").textContent = "Indexing already in progress";
          $("result-badge").textContent = "Busy";
          $("result-badge").className = "badge running";
          $("job-message").textContent = error.detail || "Wait for the current job to finish.";
        }
        return;
      }
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
    saveJobReference(data);
    renderDiff(data.diff || {});
    $("job-message").textContent = `Job ${data.id} accepted.`;
    return;
  }
  if (eventName === "progress") {
    activeJobRunning = true;
    const progress = data.progress || {};
    const ratio = Math.min(1, Math.max(0, Number(progress.ratio || 0)));
    $("progress-bar").value = ratio;
    $("progress-value").textContent = `${Math.round(ratio * 100)}%`;
    $("progress-label").textContent = `${formatNumber(progress.processed_items)} of ${formatNumber(progress.total_items)} items`;
    $("job-message").textContent = `${formatNumber(progress.completed_batches)} batches completed.`;
    updateProgressEta(progress);
    return;
  }
  if (eventName === "completed") {
    activeJobRunning = false;
    saveJobReference(data);
    renderDiff(data.diff || {});
    $("result-title").textContent = "Indexing complete";
    $("result-badge").textContent = "Complete";
    $("result-badge").className = "badge done";
    const progress = data.progress || {};
    $("progress-bar").value = 1;
    $("progress-value").textContent = "100%";
    $("progress-label").textContent = `${formatNumber(progress.processed_items)} of ${formatNumber(progress.total_items)} items`;
    $("job-message").textContent = "The dataset is ready to query.";
    $("progress-eta").textContent = "Complete";
    loadWorkspace({ quiet: true });
    return;
  }
  if (eventName === "failed") {
    activeJobRunning = false;
    saveJobReference(data);
    $("result-title").textContent = "Indexing failed";
    $("result-badge").textContent = "Failed";
    $("result-badge").className = "badge failed";
    $("job-message").textContent = data.error || "The job failed.";
    $("progress-eta").textContent = "ETA unavailable because indexing failed.";
    showError(data.error || "The indexing job failed.");
  }
}

loadWorkspace();
