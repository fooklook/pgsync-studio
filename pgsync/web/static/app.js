const state = {
    activeView: "services",
    bootstrap: null,
    editingConnectionId: null,
    editingJobId: null,
    jobEditorLocked: false,
    formLoadVersion: 0,
    clientIdCounter: 0,
    tableMetaCache: new Map(),
    sqlPreviewTimer: null,
    sqlPreviewRequestId: 0,
    jobSearchInputs: {},
    jobSearchResults: {},
    deleteJob: {
        jobId: "",
        deleteIndex: false,
    },
    indexSearch: {
        jobId: "",
        bodyText: "",
        favorites: [],
        activeFavoriteId: "",
    },
    indexSearchResult: null,
    copyJob: {
        sourceJobId: "",
        name: "",
        indexName: "",
        error: "",
    },
};
const DEFAULT_POLL_INTERVAL_SECONDS = 60;
const INDEX_SEARCH_FAVORITES_STORAGE_KEY = "pgsync_studio_index_search_favorites_v1";
const INDEX_SEARCH_FAVORITES_LIMIT = 20;

function normalizeRelationType(value) {
    if (value === "many_to_many") {
        return "many_to_one";
    }
    return value || "one_to_many";
}

async function runAction(action, errorPrefix) {
    try {
        return await action();
    } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        setStatus(`${errorPrefix}：${message}`);
        switchView("status");
        return null;
    }
}

const viewMeta = {
    services: {
        title: "基础设施配置",
        tip: "配置 Elasticsearch 和 Redis，供同步任务启动、查询和断点能力使用。",
    },
    connections: {
        title: "数据库连接",
        tip: "在线维护 PostgreSQL 连接，保存前会先做连通性校验。",
    },
    tasks: {
        title: "同步任务",
        tip: "在这里配置任务名称、索引名称、轮询间隔以及参与同步的表。",
    },
    relations: {
        title: "关联关系",
        tip: "配置父子表关系后，系统会自动生成便于理解的处理逻辑 SQL。",
    },
    "job-list": {
        title: "任务列表",
        tip: "查看任务状态，失败任务展示失败原因，可运行任务支持简单 ES 查询。",
    },
    status: {
        title: "状态信息",
        tip: "展示最近一次操作结果、任务日志摘要和调试信息。",
    },
};

const statusBox = document.getElementById("status");
const connectionList = document.getElementById("connection-list");
const tableList = document.getElementById("table-list");
const relationList = document.getElementById("relation-list");
const jobList = document.getElementById("job-list");
const sqlPreview = document.getElementById("sql-preview");
const viewTitle = document.getElementById("view-title");
const viewTip = document.getElementById("view-tip");
const assemblyFallbackCheckbox = document.getElementById("job-enable-assembly-fallback");
const assemblyFallbackLabel = assemblyFallbackCheckbox?.parentElement?.querySelector("span");
const assemblyFallbackHint = assemblyFallbackCheckbox?.closest(".job-mode-config")?.querySelector(".hint");
const durableChangeEventsCheckbox = document.getElementById("job-enable-durable-change-events");
const durableBatchSizeInput = document.getElementById("job-durable-batch-size");
const durableReplayWindowInput = document.getElementById("job-durable-replay-window");
const durableProcessedWindowInput = document.getElementById("job-durable-processed-window");
const jobModeHint = document.getElementById("job-mode-hint");
const tasksView = document.querySelector('.view[data-view="tasks"]');
const jobLockMessage = document.getElementById("job-lock-message");
const indexSearchJobSelect = document.getElementById("index-search-job");
const indexSearchNameInput = document.getElementById("index-search-name");
const indexSearchBodyInput = document.getElementById("index-search-body");
const indexAliasBodyInput = document.getElementById("index-alias-body");
const indexAliasResult = document.getElementById("index-alias-result");
const indexSearchFavorites = document.getElementById("index-search-favorites");
const indexSearchResults = document.getElementById("index-search-results");
const saveIndexSearchFavoriteButton = document.getElementById("save-index-search-favorite");
const copyJobDialog = document.getElementById("copy-job-dialog");
const copyJobForm = document.getElementById("copy-job-form");
const copyJobNameInput = document.getElementById("copy-job-name");
const copyJobIndexNameInput = document.getElementById("copy-job-index-name");
const copyJobError = document.getElementById("copy-job-error");
const copyJobCancelButton = document.getElementById("copy-job-cancel");

viewMeta.tasks = {
    title: "同步任务配置",
    tip: "在同一个页面里完成同步任务、数据表、关联关系和 SQL 预览配置。",
};
delete viewMeta.relations;
viewMeta["es-query"] = {
    title: "ES 自定义查询",
    tip: "手动维护 Elasticsearch 索引别名，或从任务列表中选择一个索引任务自定义查询索引内容。",
};

function localizeMode(value) {
    const mapping = {
        native: "Native 捕获 + Assembly 重建",
        assembly: "应用层组装",
        pending: "待处理",
    };
    return mapping[value] || value || "未设置";
}

function localizeStatus(value) {
    const mapping = {
        saved: "已保存",
        starting: "启动中",
        running: "运行中",
        stopping: "停止中",
        stopped: "已停止",
        failed: "失败",
        success: "成功",
        pending: "待处理",
    };
    return mapping[value] || value || "未设置";
}

function localizeHealthStatus(value) {
    const mapping = {
        running: "健康",
        degraded: "降级",
        recovering: "恢复中",
    };
    return mapping[value] || value || "未设置";
}

function localizePhase(value) {
    const mapping = {
        saved: "已保存",
        queued: "已入队",
        native_bootstrap: "Native 存量同步",
        native_setup: "Native 监听准备",
        native_incremental: "Native 捕获 / Assembly 重建",
        assembly_full_sync: "Assembly 存量同步",
        assembly_incremental: "Assembly 增量同步",
        cross_database_setup: "跨库增量起点准备",
        cross_database_full_sync: "跨库存量同步",
        cross_database_incremental: "跨库增量同步",
        awaiting_incremental_resume: "已完成存量，等待恢复增量",
        stopped_after_restart: "服务重启后已停止",
        stopping: "停止中",
        stopped: "已停止",
        failed: "失败",
        lease_lost: "实例租约已丢失",
    };
    return mapping[value] || value || "未设置";
}

function localizeResumeStrategy(value) {
    const mapping = {
        native_incremental: "Native 捕获增量恢复",
        assembly_incremental: "Assembly 增量恢复",
        cross_database_incremental: "跨库增量恢复",
    };
    return mapping[value] || value || "未设置";
}

function buildResumeHint(runtime) {
    if (runtime?.desired_state !== "stopped") {
        if (runtime?.runner_recovery_blocked_reason) {
            return `自动恢复已暂停，请检查配置或权限后手动启动。原因：${runtime.last_error || runtime.runner_recovery_blocked_reason}`;
        }
        if (runtime?.runner_recovery_next_attempt_ts) {
            const next = new Date(Number(runtime.runner_recovery_next_attempt_ts) * 1000).toLocaleString();
            return `等待自动恢复，第 ${runtime.runner_recovery_attempt_count || 1} 次重试预计 ${next} 执行。原因：${runtime.runner_recovery_last_error || runtime.last_error || "依赖暂时不可用"}`;
        }
        if (runtime?.runner_alive === false && ["running", "starting"].includes(runtime?.status)) {
            return "同步线程已退出，等待自动恢复检查。";
        }
    }
    if (runtime?.phase === "awaiting_incremental_resume" && runtime?.baseline_completed) {
        return "已完成存量，等待恢复增量。服务重启后可从增量续跑。";
    }
    if (runtime?.baseline_completed && runtime?.resume_strategy) {
        return `已完成存量，当前恢复策略：${localizeResumeStrategy(runtime.resume_strategy)}。`;
    }
    return "";
}

function setStatus(message, payload = null) {
    const lines = [`${new Date().toLocaleString()}  ${message}`];
    if (payload) {
        lines.push(JSON.stringify(payload, null, 2));
    }
    statusBox.textContent = lines.join("\n\n");
}

async function api(path, options = {}) {
    const response = await fetch(path, {
        method: options.method || "GET",
        headers: {
            "Content-Type": "application/json",
        },
        body: options.body ? JSON.stringify(options.body) : undefined,
    });
    const data = await response.json();
    if (!response.ok || data.ok === false) {
        throw new Error(data.error || `请求失败：${response.status}`);
    }
    return data.data;
}

function makeClientId(prefix) {
    state.clientIdCounter += 1;
    return `${prefix}_${Date.now()}_${state.clientIdCounter}`;
}

function isStaleFormLoad(loadVersion) {
    return Boolean(loadVersion) && loadVersion !== state.formLoadVersion;
}

function menuButtons() {
    return [...document.querySelectorAll(".menu-item")];
}

function switchView(view) {
    state.activeView = view;
    menuButtons().forEach((button) => {
        button.classList.toggle("active", button.dataset.view === view);
    });
    document.querySelectorAll(".view").forEach((section) => {
        section.classList.toggle("active", section.dataset.view === view);
    });
    viewTitle.textContent = viewMeta[view]?.title || "工作区";
    viewTip.textContent = viewMeta[view]?.tip || "";
}

function emptyOption(label) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = label;
    return option;
}

function fillSimpleSelect(select, values, placeholder, selectedValue = "") {
    select.innerHTML = "";
    select.appendChild(emptyOption(placeholder));
    values.forEach((value) => {
        const option = document.createElement("option");
        option.value = value;
        option.textContent = value;
        if (value === selectedValue) {
            option.selected = true;
        }
        select.appendChild(option);
    });
    if (selectedValue && !values.includes(selectedValue)) {
        const option = document.createElement("option");
        option.value = selectedValue;
        option.textContent = selectedValue;
        option.selected = true;
        select.appendChild(option);
    }
}

function fillObjectSelect(select, items, placeholder, selectedValue = "") {
    select.innerHTML = "";
    select.appendChild(emptyOption(placeholder));
    items.forEach((item) => {
        const option = document.createElement("option");
        option.value = item.value;
        option.textContent = item.label;
        if (item.value === selectedValue) {
            option.selected = true;
        }
        select.appendChild(option);
    });
}

function getConnections() {
    return state.bootstrap?.connections || [];
}

function getJobs() {
    return state.bootstrap?.jobs || [];
}

function getJobRuntime(jobId) {
    return state.bootstrap?.job_runtime?.[jobId] || {};
}

function isLockedJob(job) {
    return Boolean(job?.id && getJobRuntime(job.id)?.job_locked);
}

function setCopyJobError(message = "") {
    state.copyJob.error = message;
    if (!copyJobError) {
        return;
    }
    copyJobError.textContent = message;
    copyJobError.hidden = !message;
}

function resetCopyJobState() {
    state.copyJob = {
        sourceJobId: "",
        name: "",
        indexName: "",
        error: "",
    };
    if (copyJobNameInput) {
        copyJobNameInput.value = "";
    }
    if (copyJobIndexNameInput) {
        copyJobIndexNameInput.value = "";
    }
    setCopyJobError("");
}

function hasJobNameConflict(name, excludeJobId = "") {
    const normalized = name.trim();
    return getJobs().some(
        (job) => job.id !== excludeJobId && (job.name || "").trim() === normalized
    );
}

function hasJobIndexConflict(indexName, excludeJobId = "") {
    const normalized = indexName.trim();
    return getJobs().some(
        (job) => job.id !== excludeJobId && (job.index_name || "").trim() === normalized
    );
}

function closeCopyJobDialog() {
    if (!copyJobDialog) {
        resetCopyJobState();
        return;
    }
    if (copyJobDialog.open && typeof copyJobDialog.close === "function") {
        copyJobDialog.close();
    } else {
        copyJobDialog.removeAttribute("open");
    }
    resetCopyJobState();
}

function openCopyJobDialog(job) {
    if (!job || !copyJobDialog) {
        return;
    }
    state.copyJob = {
        sourceJobId: job.id,
        name: job.name || "",
        indexName: job.index_name || "",
        error: "",
    };
    copyJobNameInput.value = state.copyJob.name;
    copyJobIndexNameInput.value = state.copyJob.indexName;
    setCopyJobError("");
    if (typeof copyJobDialog.showModal === "function") {
        copyJobDialog.showModal();
    } else {
        copyJobDialog.setAttribute("open", "open");
    }
    window.requestAnimationFrame(() => {
        copyJobNameInput?.focus();
        copyJobNameInput?.select();
    });
}

function applyJobEditorLock(locked, job = null) {
    state.jobEditorLocked = Boolean(locked);
    if (jobLockMessage) {
        if (locked && job?.name) {
            jobLockMessage.textContent = `任务 ${job.name} 的索引已生成，当前任务已锁定，不允许再修改任何字段。请返回任务列表使用“复制”创建新任务。`;
            jobLockMessage.hidden = false;
        } else {
            jobLockMessage.textContent = "";
            jobLockMessage.hidden = true;
        }
    }
    if (!tasksView) {
        return;
    }
    const controls = tasksView.querySelectorAll("input, select, textarea, button");
    controls.forEach((control) => {
        if (locked) {
            control.dataset.lockRestoreDisabled = control.disabled ? "true" : "false";
            control.disabled = true;
            return;
        }
        if (Object.prototype.hasOwnProperty.call(control.dataset, "lockRestoreDisabled")) {
            control.disabled = control.dataset.lockRestoreDisabled === "true";
            delete control.dataset.lockRestoreDisabled;
        }
    });
}

function defaultIndexSearchBody() {
    return JSON.stringify(
        {
            size: 10,
            query: {
                match_all: {},
            },
        },
        null,
        2
    );
}

function loadIndexSearchFavorites() {
    try {
        const raw = window.localStorage.getItem(INDEX_SEARCH_FAVORITES_STORAGE_KEY);
        const parsed = raw ? JSON.parse(raw) : [];
        state.indexSearch.favorites = Array.isArray(parsed)
            ? parsed.filter((item) => item && item.id && item.name && item.body)
            : [];
    } catch (error) {
        state.indexSearch.favorites = [];
    }
}

function persistIndexSearchFavorites() {
    window.localStorage.setItem(
        INDEX_SEARCH_FAVORITES_STORAGE_KEY,
        JSON.stringify(state.indexSearch.favorites.slice(0, INDEX_SEARCH_FAVORITES_LIMIT))
    );
}

function formatFavoriteTime(value) {
    if (!value) {
        return "";
    }
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
        return "";
    }
    return date.toLocaleString();
}

function getCurrentIndexSearchJob() {
    return getJobs().find((job) => job.id === state.indexSearch.jobId) || null;
}

function applyIndexSearchFavorite(favoriteId) {
    const favorite = state.indexSearch.favorites.find((item) => item.id === favoriteId);
    if (!favorite) {
        return;
    }
    if (favorite.jobId && getJobs().some((job) => job.id === favorite.jobId)) {
        state.indexSearch.jobId = favorite.jobId;
        indexSearchJobSelect.value = favorite.jobId;
        const currentJob = getCurrentIndexSearchJob();
        indexSearchNameInput.value = currentJob?.index_name || favorite.indexName || "";
    }
    state.indexSearch.activeFavoriteId = favorite.id;
    state.indexSearch.bodyText = favorite.body;
    indexSearchBodyInput.value = favorite.body;
    renderIndexSearchFavorites();
    renderIndexSearchMessage(`已应用收藏：${favorite.name}`);
    switchView("es-query");
}

function removeIndexSearchFavorite(favoriteId) {
    const favorite = state.indexSearch.favorites.find((item) => item.id === favoriteId);
    state.indexSearch.favorites = state.indexSearch.favorites.filter((item) => item.id !== favoriteId);
    if (state.indexSearch.activeFavoriteId === favoriteId) {
        state.indexSearch.activeFavoriteId = "";
    }
    persistIndexSearchFavorites();
    renderIndexSearchFavorites();
    if (favorite) {
        renderIndexSearchMessage(`已删除收藏：${favorite.name}`);
    }
}

function renderIndexSearchFavorites() {
    if (!indexSearchFavorites) {
        return;
    }
    indexSearchFavorites.innerHTML = "";
    if (!state.indexSearch.favorites.length) {
        const empty = document.createElement("div");
        empty.className = "empty-state";
        empty.textContent = "还没有收藏的 Query DSL JSON。";
        indexSearchFavorites.appendChild(empty);
        return;
    }

    const currentJobId = state.indexSearch.jobId;
    const favorites = [...state.indexSearch.favorites].sort((a, b) => {
        const aCurrent = a.jobId === currentJobId ? 1 : 0;
        const bCurrent = b.jobId === currentJobId ? 1 : 0;
        if (aCurrent !== bCurrent) {
            return bCurrent - aCurrent;
        }
        return String(b.updatedAt || "").localeCompare(String(a.updatedAt || ""));
    });

    favorites.forEach((favorite) => {
        const row = document.createElement("div");
        row.className = "favorite-item";
        if (favorite.id === state.indexSearch.activeFavoriteId) {
            row.classList.add("active");
        }

        const applyButton = document.createElement("button");
        applyButton.type = "button";
        applyButton.className = "favorite-apply";
        applyButton.addEventListener("click", () => {
            applyIndexSearchFavorite(favorite.id);
        });

        const name = document.createElement("span");
        name.className = "favorite-name";
        name.textContent = favorite.name;

        const meta = document.createElement("span");
        meta.className = "favorite-meta";
        const metaParts = [];
        if (favorite.jobName) {
            metaParts.push(favorite.jobName);
        } else if (favorite.indexName) {
            metaParts.push(favorite.indexName);
        }
        const formattedTime = formatFavoriteTime(favorite.updatedAt);
        if (formattedTime) {
            metaParts.push(formattedTime);
        }
        meta.textContent = metaParts.join(" / ");

        applyButton.appendChild(name);
        applyButton.appendChild(meta);

        const actions = document.createElement("div");
        actions.className = "favorite-actions";

        const removeButton = document.createElement("button");
        removeButton.type = "button";
        removeButton.className = "danger favorite-remove";
        removeButton.textContent = "删除";
        removeButton.addEventListener("click", () => {
            removeIndexSearchFavorite(favorite.id);
        });

        actions.appendChild(removeButton);
        row.appendChild(applyButton);
        row.appendChild(actions);
        indexSearchFavorites.appendChild(row);
    });
}

function saveCurrentIndexSearchFavorite() {
    const rawBody = indexSearchBodyInput.value.trim();
    if (!rawBody) {
        renderIndexSearchMessage("当前 Query DSL JSON 为空，无法加入收藏。", true);
        switchView("es-query");
        return;
    }

    let body = {};
    try {
        body = JSON.parse(rawBody);
    } catch (error) {
        renderIndexSearchMessage(`当前 Query DSL JSON 不是合法 JSON：${error.message}`, true);
        switchView("es-query");
        return;
    }

    const currentJob = getCurrentIndexSearchJob();
    const defaultName = currentJob
        ? `${currentJob.name}-${new Date().toLocaleString()}`
        : `Query-${new Date().toLocaleString()}`;
    const inputName = window.prompt("请输入收藏名称", defaultName);
    if (inputName === null) {
        return;
    }
    const favoriteName = inputName.trim();
    if (!favoriteName) {
        renderIndexSearchMessage("收藏名称不能为空。", true);
        switchView("es-query");
        return;
    }

    const normalizedBody = JSON.stringify(body, null, 2);
    const existing = state.indexSearch.favorites.find(
        (item) => item.name === favoriteName && (item.jobId || "") === (currentJob?.id || "")
    );
    const favorite = {
        id: existing?.id || makeClientId("index_favorite"),
        name: favoriteName,
        jobId: currentJob?.id || "",
        jobName: currentJob?.name || "",
        indexName: currentJob?.index_name || indexSearchNameInput.value.trim(),
        body: normalizedBody,
        updatedAt: new Date().toISOString(),
    };

    state.indexSearch.bodyText = normalizedBody;
    indexSearchBodyInput.value = normalizedBody;
    state.indexSearch.activeFavoriteId = favorite.id;
    state.indexSearch.favorites = [
        favorite,
        ...state.indexSearch.favorites.filter((item) => item.id !== favorite.id),
    ].slice(0, INDEX_SEARCH_FAVORITES_LIMIT);
    persistIndexSearchFavorites();
    renderIndexSearchFavorites();
    renderIndexSearchMessage(`已收藏 Query DSL：${favorite.name}`);
    switchView("es-query");
}

function serviceFields() {
    return {
        search: {
            url: document.getElementById("search-url").value.trim(),
            username: document.getElementById("search-username").value.trim(),
            password: document.getElementById("search-password").value,
            verify_certs: document.getElementById("search-verify-certs").checked,
        },
        redis: {
            host: document.getElementById("redis-host").value.trim(),
            port: Number(document.getElementById("redis-port").value || 6379),
            password: document.getElementById("redis-password").value,
            db: Number(document.getElementById("redis-db").value || 0),
        },
    };
}

function fillServices(services) {
    document.getElementById("search-url").value = services?.search?.url || "";
    document.getElementById("search-username").value = services?.search?.username || "";
    document.getElementById("search-password").value = services?.search?.password || "";
    document.getElementById("search-verify-certs").checked = Boolean(services?.search?.verify_certs);
    document.getElementById("redis-host").value = services?.redis?.host || "";
    document.getElementById("redis-port").value = services?.redis?.port || 6379;
    document.getElementById("redis-password").value = services?.redis?.password || "";
    document.getElementById("redis-db").value = services?.redis?.db ?? 0;
}

function connectionFields() {
    return {
        id: state.editingConnectionId,
        name: document.getElementById("connection-name").value.trim(),
        host: document.getElementById("connection-host").value.trim(),
        port: Number(document.getElementById("connection-port").value || 5432),
        database: document.getElementById("connection-database").value.trim(),
        username: "******",
        password: "******",
        type: "postgresql",
    };
}

function fillConnectionForm(connection) {
    state.editingConnectionId = connection?.id || null;
    document.getElementById("connection-name").value = connection?.name || "";
    document.getElementById("connection-host").value = connection?.host || "";
    document.getElementById("connection-port").value = connection?.port || 5432;
    document.getElementById("connection-database").value = connection?.database || "";
    document.getElementById("connection-username").value = "******";
    document.getElementById("connection-password").value = "******";
}

function renderConnections() {
    const connections = getConnections();
    connectionList.innerHTML = "";
    if (!connections.length) {
        connectionList.innerHTML = "<div class='empty-state'>当前还没有保存过数据库连接。</div>";
        return;
    }
    connections.forEach((connection) => {
        const card = document.createElement("div");
        card.className = "row-block";
        card.innerHTML = `
            <div class="job-card">
                <strong>${connection.name}</strong>
                <div class="job-meta">
                    <span>${connection.host}:${connection.port}</span>
                    <span>${connection.database}</span>
                    <span>服务端共享凭据</span>
                </div>
                <div class="actions">
                    <button class="ghost use-connection" type="button">加载</button>
                    <button class="danger delete-connection" type="button">删除</button>
                </div>
            </div>
        `;
        card.querySelector(".use-connection").addEventListener("click", () => {
            fillConnectionForm(connection);
            switchView("connections");
            setStatus(`已加载连接：${connection.name}`);
        });
        card.querySelector(".delete-connection").addEventListener("click", async () => {
            await api(`/api/connections/${connection.id}`, { method: "DELETE" });
            await loadBootstrap();
            setStatus(`已删除连接：${connection.name}`);
        });
        connectionList.appendChild(card);
    });
}

function fillConnectionOptions(select, selectedValue = "") {
    fillObjectSelect(
        select,
        getConnections().map((connection) => ({
            value: connection.id,
            label: connection.name,
        })),
        "请选择连接",
        selectedValue
    );
}

function fillIndexSearchJobOptions() {
    const jobs = getJobs();
    const currentJobId = state.indexSearch.jobId;
    fillObjectSelect(
        indexSearchJobSelect,
        jobs.map((job) => ({
            value: job.id,
            label: `${job.name} (${job.index_name})`,
        })),
        "请选择任务索引",
        currentJobId
    );
    if (!jobs.length) {
        state.indexSearch.jobId = "";
        indexSearchJobSelect.value = "";
        indexSearchNameInput.value = "";
        return;
    }
    if (!jobs.some((job) => job.id === state.indexSearch.jobId)) {
        state.indexSearch.jobId = jobs[0].id;
    }
    indexSearchJobSelect.value = state.indexSearch.jobId;
    const currentJob = jobs.find((job) => job.id === state.indexSearch.jobId);
    indexSearchNameInput.value = currentJob?.index_name || "";
}

function renderIndexSearchMessage(message, isError = false) {
    indexSearchResults.innerHTML = "";
    const div = document.createElement("div");
    div.className = isError ? "inline-alert error" : "empty-state";
    div.textContent = message;
    indexSearchResults.appendChild(div);
}

function renderIndexSearchResults(result) {
    indexSearchResults.innerHTML = "";
    const job = result?.job || {};
    const summary = document.createElement("div");
    summary.className = "inline-alert info";
    if (result?.exists === false) {
        summary.textContent = `索引 ${job.index_name || "-"} 还不存在。`;
        indexSearchResults.appendChild(summary);
        return;
    }
    summary.textContent = `任务 ${job.name || "-"}，索引 ${job.index_name || "-"}，共命中 ${result.total || 0} 条，当前返回 ${(result.hits || []).length} 条，用时 ${result.took || 0} ms。`;
    indexSearchResults.appendChild(summary);

    if (!result?.hits?.length) {
        const empty = document.createElement("div");
        empty.className = "empty-state";
        empty.textContent = "没有查询到符合条件的数据。";
        indexSearchResults.appendChild(empty);
        return;
    }

    result.hits.forEach((hit) => {
        const card = document.createElement("div");
        card.className = "result-card";
        card.innerHTML = `
            <div class="result-head">
                <span>文档 ID：${hit.id || "-"}</span>
                <span>Score：${hit.score ?? "-"}</span>
            </div>
            <pre class="result-json"></pre>
        `;
        card.querySelector(".result-json").textContent = JSON.stringify(hit.source || {}, null, 2);
        indexSearchResults.appendChild(card);
    });
}

function tableCacheKey(connectionId, schema, table) {
    return `${connectionId}::${schema}::${table}`;
}

async function loadTableMeta(connectionId, schema, table) {
    const key = tableCacheKey(connectionId, schema, table);
    if (!state.tableMetaCache.has(key)) {
        state.tableMetaCache.set(
            key,
            api(
                `/api/connections/${connectionId}/columns?schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`
            )
        );
    }
    return state.tableMetaCache.get(key);
}

function tableBlocks() {
    return [...document.querySelectorAll(".table-block")];
}

function relationBlocks() {
    return [...document.querySelectorAll(".relation-block")];
}

function currentTables() {
    return tableBlocks().map((block) => {
        const table = block.querySelector('[data-field="table"]').value;
        return {
            id: block.dataset.id || makeClientId("table"),
            alias: block.querySelector('[data-field="alias"]').value.trim() || table,
            connection_id: block.querySelector('[data-field="connection_id"]').value,
            schema: block.querySelector('[data-field="schema"]').value,
            table,
            primary_key: block.querySelector('[data-field="primary_key"]').value
                .split(",")
                .map((item) => item.trim())
                .filter(Boolean),
            cursor_column: block.querySelector('[data-field="cursor_column"]').value.trim(),
            soft_delete_column: block.querySelector('[data-field="soft_delete_column"]').value.trim(),
        };
    });
}

function jobUsesCrossDatabase(job = {}) {
    const connectionIds = new Set(
        (job.tables || [])
            .map((table) => table.connection_id)
            .filter(Boolean)
    );
    return connectionIds.size > 1;
}

function resolvePollIntervalSeconds(value) {
    const numeric = Number(value);
    return Number.isFinite(numeric) && numeric > 0
        ? numeric
        : DEFAULT_POLL_INTERVAL_SECONDS;
}

function formatMetricNumber(value) {
    const numeric = Number(value || 0);
    return Number.isFinite(numeric) ? numeric.toLocaleString() : "0";
}

function queueMetricText(runtime = {}) {
    const total = Number(runtime.assembly_root_queue_size || 0);
    const hot = Number(runtime.assembly_hot_queue_size || 0);
    const backlog = Number(runtime.assembly_backlog_queue_size || 0);
    const inflight = Number(runtime.assembly_inflight_batch_size || 0);
    return `Queue: ${formatMetricNumber(total)} | hot ${formatMetricNumber(hot)} | backlog ${formatMetricNumber(backlog)} | inflight ${formatMetricNumber(inflight)}`;
}

function lastBatchMetricText(runtime = {}) {
    const raw = Number(
        runtime.assembly_last_batch_raw_root_key_count
        ?? runtime.assembly_last_batch_root_key_count
        ?? 0
    );
    const deduped = Number(
        runtime.assembly_last_batch_deduped_root_key_count ?? raw
    );
    const workers = Number(runtime.assembly_last_batch_worker_count || 0);
    const elapsed = Number(runtime.assembly_last_batch_elapsed_ms || 0);
    if (!raw && !deduped && !workers && !elapsed) {
        return "";
    }
    return `Last batch: raw ${formatMetricNumber(raw)} | deduped ${formatMetricNumber(deduped)} | workers ${formatMetricNumber(workers || 1)} | ${formatMetricNumber(elapsed)} ms`;
}

function crossDatabaseStrategyLabel(job = {}) {
    if (job.enable_assembly_fallback) {
        return `Assembly 存量 + Native 变更捕获 + Assembly 重建 + 周期性 Recovery Scan（${resolvePollIntervalSeconds(job.poll_interval_seconds)}s）`;
    }
    return "Assembly 存量 + Native 变更捕获 + Assembly 重建";
}

function refreshAssemblyFallbackHint(job = {}) {
    const enabled = Boolean(job.enable_assembly_fallback);
    const durableEnabled = Boolean(job.enable_durable_change_events);
    const pollIntervalSeconds = resolvePollIntervalSeconds(job.poll_interval_seconds);
    if (assemblyFallbackLabel) {
        assemblyFallbackLabel.textContent = "启用周期性 Recovery Scan 兜底";
    }
    if (assemblyFallbackHint) {
        assemblyFallbackHint.textContent = `勾选后，系统会在 Native 监听变更之外，再按轮询方式执行恢复扫描与 Assembly 补偿重建；默认轮询间隔为 ${DEFAULT_POLL_INTERVAL_SECONDS} 秒。`;
    }
    if (assemblyFallbackCheckbox) {
        assemblyFallbackCheckbox.checked = enabled;
        assemblyFallbackCheckbox.disabled = false;
    }
    if (durableChangeEventsCheckbox) {
        durableChangeEventsCheckbox.checked = durableEnabled;
    }
    jobModeHint.className = "inline-alert info";
    const durableText = durableEnabled
        ? "已开启 durable outbox，触发器会在同一事务写入可靠事件表，PG_NOTIFY 只负责唤醒。"
        : "未开启 durable outbox，PG_NOTIFY 仍作为实时事件来源。";
    if (enabled) {
        jobModeHint.textContent = `系统会先执行 Assembly full_sync；增量阶段由 Native 监听数据库变更并解析 root key，再由 Assembly 回源重建文档。${durableText} 当前已开启周期性 Recovery Scan，每 ${pollIntervalSeconds} 秒会执行一次补漏扫描。`;
        return;
    }
    jobModeHint.textContent = `系统会先执行 Assembly full_sync；增量阶段由 Native 监听数据库变更并解析 root key，再由 Assembly 回源重建文档。${durableText} 当前仅保留事件驱动恢复扫描，未开启周期性 Recovery Scan。`;
}

function findTableById(tableId) {
    return currentTables().find((table) => table.id === tableId);
}

async function populateSchemaOptions(block, selectedValue = "") {
    const connectionId = block.querySelector('[data-field="connection_id"]').value;
    const schemaSelect = block.querySelector('[data-field="schema"]');
    const tableSelect = block.querySelector('[data-field="table"]');
    const cursorSelect = block.querySelector('[data-field="cursor_column"]');
    const softDeleteSelect = block.querySelector('[data-field="soft_delete_column"]');
    block.querySelector('[data-field="primary_key"]').value = "";
    fillSimpleSelect(schemaSelect, [], "请选择 Schema");
    fillSimpleSelect(tableSelect, [], "请选择表");
    fillSimpleSelect(cursorSelect, [], "请选择游标字段");
    fillSimpleSelect(softDeleteSelect, [], "请选择软删除字段");
    updateSoftDeleteFieldState(block, "");
    if (!connectionId) {
        return;
    }
    const schemas = await api(`/api/connections/${connectionId}/schemas`);
    fillSimpleSelect(schemaSelect, schemas, "请选择 Schema", selectedValue);
}

async function populateTableOptions(block, selectedSchema = "", selectedTable = "") {
    const connectionId = block.querySelector('[data-field="connection_id"]').value;
    const schema = selectedSchema || block.querySelector('[data-field="schema"]').value;
    const tableSelect = block.querySelector('[data-field="table"]');
    const cursorSelect = block.querySelector('[data-field="cursor_column"]');
    const softDeleteSelect = block.querySelector('[data-field="soft_delete_column"]');
    block.querySelector('[data-field="primary_key"]').value = "";
    fillSimpleSelect(tableSelect, [], "请选择表");
    fillSimpleSelect(cursorSelect, [], "请选择游标字段");
    fillSimpleSelect(softDeleteSelect, [], "请选择软删除字段");
    updateSoftDeleteFieldState(block, "");
    if (!connectionId || !schema) {
        return;
    }
    const tables = await api(`/api/connections/${connectionId}/tables?schema=${encodeURIComponent(schema)}`);
    fillSimpleSelect(tableSelect, tables, "请选择表", selectedTable);
    if (selectedTable) {
        await populateTableMetadata(block, schema, selectedTable);
    }
}

function fillColumnSelect(select, columns, placeholder, selectedValue = "", suggestedValue = "") {
    const names = columns.map((column) => column.name);
    fillSimpleSelect(select, names, placeholder, selectedValue || suggestedValue);
    if (!selectedValue && suggestedValue && names.includes(suggestedValue)) {
        select.value = suggestedValue;
    }
}

function normalizeSoftDeleteCandidate(value) {
    return String(value || "").trim();
}

function resolveSoftDeleteSuggestion(meta = {}) {
    const explicit = normalizeSoftDeleteCandidate(meta.soft_delete_column);
    if (explicit) {
        return explicit;
    }
    const deletedColumn = (meta.columns || []).find(
        (column) => normalizeSoftDeleteCandidate(column?.name).toLowerCase() === "deleted"
    );
    return normalizeSoftDeleteCandidate(deletedColumn?.name);
}

function updateSoftDeleteFieldState(block, suggestedValue = null) {
    const status = block.querySelector('[data-role="soft-delete-current"]');
    const select = block.querySelector('[data-field="soft_delete_column"]');
    if (!status || !select) {
        return;
    }
    if (suggestedValue !== null) {
        block.dataset.softDeleteSuggested = normalizeSoftDeleteCandidate(suggestedValue);
    }
    const selected = normalizeSoftDeleteCandidate(select.value);
    const suggested = normalizeSoftDeleteCandidate(block.dataset.softDeleteSuggested);
    let message = "未设置";
    if (selected) {
        message = `当前值：${selected}`;
    } else if (suggested) {
        message = `自动识别：${suggested}`;
    }
    status.textContent = message;
    status.title = message;
    status.classList.toggle("active", Boolean(selected || suggested));
    status.classList.toggle("muted", !selected && !suggested);
}

async function populateTableMetadata(block, selectedSchema = "", selectedTable = "") {
    const connectionId = block.querySelector('[data-field="connection_id"]').value;
    const schema = selectedSchema || block.querySelector('[data-field="schema"]').value;
    const table = selectedTable || block.querySelector('[data-field="table"]').value;
    if (!connectionId || !schema || !table) {
        return;
    }
    const meta = await loadTableMeta(connectionId, schema, table);
    const aliasInput = block.querySelector('[data-field="alias"]');
    const currentCursor = block.querySelector('[data-field="cursor_column"]').value;
    const currentSoftDelete = block.querySelector('[data-field="soft_delete_column"]').value;
    if (!aliasInput.value.trim()) {
        aliasInput.value = table;
    }
    block.querySelector('[data-field="primary_key"]').value = (meta.primary_key || []).join(",");
    const suggestedSoftDelete = resolveSoftDeleteSuggestion(meta);
    fillColumnSelect(
        block.querySelector('[data-field="cursor_column"]'),
        meta.columns || [],
        "请选择游标字段",
        currentCursor,
        meta.cursor_column || ""
    );
    fillColumnSelect(
        block.querySelector('[data-field="soft_delete_column"]'),
        meta.columns || [],
        "请选择软删除字段",
        currentSoftDelete,
        suggestedSoftDelete
    );
    updateSoftDeleteFieldState(block, suggestedSoftDelete);
}

function addTableBlock(table = {}, loadVersion = 0) {
    const template = document.getElementById("table-template");
    const block = template.content.firstElementChild.cloneNode(true);
    block.dataset.id = table.id || makeClientId("table");
    block.querySelector('[data-field="alias"]').value = table.alias || "";
    block.querySelector('[data-field="primary_key"]').value = (table.primary_key || []).join(",");
    fillConnectionOptions(block.querySelector('[data-field="connection_id"]'), table.connection_id || "");
    fillSimpleSelect(block.querySelector('[data-field="schema"]'), [], "请选择 Schema", table.schema || "");
    fillSimpleSelect(block.querySelector('[data-field="table"]'), [], "请选择表", table.table || "");
    fillSimpleSelect(block.querySelector('[data-field="cursor_column"]'), [], "请选择游标字段", table.cursor_column || "");

    fillSimpleSelect(
        block.querySelector('[data-field="soft_delete_column"]'),
        [],
        "请选择软删除字段",
        table.soft_delete_column || ""
    );
    updateSoftDeleteFieldState(block, table.soft_delete_column || "");

    block.querySelector('[data-field="alias"]').addEventListener("input", () => {
        void refreshRelationOptions();
        scheduleSqlPreview();
    });
    block.querySelector('[data-field="connection_id"]').addEventListener("change", async () => {
        await populateSchemaOptions(block);
        await refreshRelationOptions();
        scheduleSqlPreview();
    });
    block.querySelector('[data-field="schema"]').addEventListener("change", async () => {
        await populateTableOptions(block);
        await refreshRelationOptions();
        scheduleSqlPreview();
    });
    block.querySelector('[data-field="table"]').addEventListener("change", async () => {
        await populateTableMetadata(block);
        await refreshRelationOptions();
        scheduleSqlPreview();
    });
    block.querySelector('[data-field="cursor_column"]').addEventListener("change", () => {
        scheduleSqlPreview();
    });
    block.querySelector('[data-field="soft_delete_column"]').addEventListener("change", () => {
        updateSoftDeleteFieldState(block);
        scheduleSqlPreview();
    });
    block.querySelector(".remove-row").addEventListener("click", () => {
        block.remove();
        void refreshRelationOptions();
        scheduleSqlPreview();
    });

    tableList.appendChild(block);

    return (async () => {
        await populateSchemaOptions(block, table.schema || "");
        if (isStaleFormLoad(loadVersion)) {
            return block;
        }
        if (table.schema) {
            await populateTableOptions(block, table.schema, table.table || "");
            if (isStaleFormLoad(loadVersion)) {
                return block;
            }
        }
        if (table.table) {
            await populateTableMetadata(block, table.schema || "", table.table);
            if (isStaleFormLoad(loadVersion)) {
                return block;
            }
            if (table.cursor_column) {
                block.querySelector('[data-field="cursor_column"]').value = table.cursor_column;
            }
            if (table.soft_delete_column) {
                block.querySelector('[data-field="soft_delete_column"]').value = table.soft_delete_column;
            }
            updateSoftDeleteFieldState(block);
        }
        await refreshRelationOptions();
        if (!isStaleFormLoad(loadVersion)) {
            scheduleSqlPreview();
        }
        return block;
    })();
}

function relationTableOptions() {
    return currentTables()
        .filter((table) => table.table)
        .map((table) => ({
            value: table.id,
            label: `${table.alias} (${table.schema}.${table.table})`,
        }));
}

function addMappingRow(container, pair = {}) {
    const template = document.getElementById("mapping-template");
    const row = template.content.firstElementChild.cloneNode(true);
    row.querySelector(".remove-mapping").addEventListener("click", () => {
        row.remove();
        scheduleSqlPreview();
    });
    row.querySelector('[data-field="parent_field"]').addEventListener("change", () => {
        scheduleSqlPreview();
    });
    row.querySelector('[data-field="child_field"]').addEventListener("change", () => {
        scheduleSqlPreview();
    });
    container.appendChild(row);
    row.dataset.parentField = pair.parent_field || "";
    row.dataset.childField = pair.child_field || "";
    return row;
}

function fillColumnSelectWithTypes(select, columns, placeholder, selectedValue = "") {
    select.innerHTML = "";
    select.appendChild(emptyOption(placeholder));
    columns.forEach((column) => {
        const option = document.createElement("option");
        option.value = column.name;
        option.textContent = column.name;
        option.dataset.type = column.type || "";
        if (column.name === selectedValue) {
            option.selected = true;
        }
        select.appendChild(option);
    });
}

function parseConditionValue(rawValue, fieldType = "") {
    const value = rawValue.trim();
    if (!value) {
        return "";
    }
    if (/bool/i.test(fieldType)) {
        if (value.toLowerCase() === "true") {
            return true;
        }
        if (value.toLowerCase() === "false") {
            return false;
        }
    }
    if (/(int|numeric|decimal|real|double|float|serial)/i.test(fieldType)) {
        const parsed = Number(value);
        if (!Number.isNaN(parsed)) {
            return parsed;
        }
    }
    return value;
}

function normalizeConditionSource(value) {
    return String(value || "").trim().toLowerCase() === "parent" ? "parent" : "child";
}

function updateConditionFieldLabel(row, source) {
    const label = row.querySelector('[data-role="condition-field-label"]');
    if (!label) {
        return;
    }
    const textNode = [...label.childNodes].find((node) => node.nodeType === Node.TEXT_NODE);
    if (textNode) {
        textNode.nodeValue = source === "parent" ? "父表字段" : "子表字段";
    }
}

function addConditionRow(container, condition = {}) {
    const template = document.getElementById("condition-template");
    const row = template.content.firstElementChild.cloneNode(true);
    const source = normalizeConditionSource(condition.source);
    row.dataset.field = condition.field || "";
    row.dataset.fieldType = condition.field_type || "";
    row.dataset.fieldSource = source;
    row.querySelector('[data-field="source"]').value = source;
    row.querySelector('[data-field="operator"]').value = condition.operator || "eq";
    row.querySelector('[data-field="value"]').value = condition.value ?? "";
    updateConditionFieldLabel(row, source);
    row.querySelector(".remove-condition").addEventListener("click", () => {
        row.remove();
        scheduleSqlPreview();
    });
    row.querySelector('[data-field="source"]').addEventListener("change", async () => {
        const nextSource = normalizeConditionSource(
            row.querySelector('[data-field="source"]').value
        );
        row.dataset.fieldSource = nextSource;
        updateConditionFieldLabel(row, nextSource);
        await refreshConditionOptions(row.closest(".relation-block"));
        scheduleSqlPreview();
    });
    row.querySelector('[data-field="field"]').addEventListener("change", () => {
        const option = row.querySelector('[data-field="field"]').selectedOptions[0];
        row.dataset.field = row.querySelector('[data-field="field"]').value;
        row.dataset.fieldType = option?.dataset.type || "";
        scheduleSqlPreview();
    });
    row.querySelector('[data-field="operator"]').addEventListener("change", () => {
        scheduleSqlPreview();
    });
    row.querySelector('[data-field="value"]').addEventListener("input", () => {
        scheduleSqlPreview();
    });
    container.appendChild(row);
    return row;
}

async function refreshMappingOptions(block) {
    const parentId = block.querySelector('[data-field="parent_table_id"]').value;
    const childId = block.querySelector('[data-field="child_table_id"]').value;
    const parentTable = findTableById(parentId);
    const childTable = findTableById(childId);
    const rows = [...block.querySelectorAll(".mapping-row")];
    if (!parentTable || !childTable) {
        rows.forEach((row) => {
            fillSimpleSelect(row.querySelector('[data-field="parent_field"]'), [], "请选择父表字段");
            fillSimpleSelect(row.querySelector('[data-field="child_field"]'), [], "请选择子表字段");
        });
        return;
    }
    const [parentMeta, childMeta] = await Promise.all([
        loadTableMeta(parentTable.connection_id, parentTable.schema, parentTable.table),
        loadTableMeta(childTable.connection_id, childTable.schema, childTable.table),
    ]);
    rows.forEach((row) => {
        const parentSelect = row.querySelector('[data-field="parent_field"]');
        const childSelect = row.querySelector('[data-field="child_field"]');
        const currentParent = parentSelect.value || row.dataset.parentField || "";
        const currentChild = childSelect.value || row.dataset.childField || "";
        fillColumnSelect(parentSelect, parentMeta.columns || [], "请选择父表字段", currentParent);
        fillColumnSelect(childSelect, childMeta.columns || [], "请选择子表字段", currentChild);
        row.dataset.parentField = parentSelect.value;
        row.dataset.childField = childSelect.value;
    });
}

async function refreshConditionOptionsLegacyOnly(block) {
    const childId = block.querySelector('[data-field="child_table_id"]').value;
    const childTable = findTableById(childId);
    const rows = [...block.querySelectorAll(".condition-row")];
    if (!childTable) {
        rows.forEach((row) => {
            fillSimpleSelect(row.querySelector('[data-field="field"]'), [], "请选择子表字段");
            row.dataset.fieldType = "";
        });
        return;
    }
    const childMeta = await loadTableMeta(
        childTable.connection_id,
        childTable.schema,
        childTable.table
    );
    rows.forEach((row) => {
        const fieldSelect = row.querySelector('[data-field="field"]');
        const currentField = fieldSelect.value || row.dataset.field || "";
        fillColumnSelectWithTypes(
            fieldSelect,
            childMeta.columns || [],
            "请选择子表字段",
            currentField
        );
        const option = fieldSelect.selectedOptions[0];
        row.dataset.field = fieldSelect.value;
        row.dataset.fieldType = option?.dataset.type || "";
    });
}

async function refreshConditionOptions(block) {
    const parentId = block.querySelector('[data-field="parent_table_id"]').value;
    const childId = block.querySelector('[data-field="child_table_id"]').value;
    const parentTable = findTableById(parentId);
    const childTable = findTableById(childId);
    const rows = [...block.querySelectorAll(".condition-row")];
    if (!rows.length) {
        return;
    }
    const needsParentMeta = rows.some(
        (row) => normalizeConditionSource(
            row.querySelector('[data-field="source"]').value || row.dataset.fieldSource
        ) === "parent"
    );
    const needsChildMeta = rows.some(
        (row) => normalizeConditionSource(
            row.querySelector('[data-field="source"]').value || row.dataset.fieldSource
        ) === "child"
    );
    const [parentMeta, childMeta] = await Promise.all([
        needsParentMeta && parentTable
            ? loadTableMeta(parentTable.connection_id, parentTable.schema, parentTable.table)
            : Promise.resolve(null),
        needsChildMeta && childTable
            ? loadTableMeta(childTable.connection_id, childTable.schema, childTable.table)
            : Promise.resolve(null),
    ]);
    rows.forEach((row) => {
        const source = normalizeConditionSource(
            row.querySelector('[data-field="source"]').value || row.dataset.fieldSource
        );
        const fieldSelect = row.querySelector('[data-field="field"]');
        const currentField = fieldSelect.value || row.dataset.field || "";
        const columns = source === "parent"
            ? (parentMeta?.columns || [])
            : (childMeta?.columns || []);
        const placeholder = source === "parent" ? "请选择父表字段" : "请选择子表字段";
        updateConditionFieldLabel(row, source);
        fillColumnSelectWithTypes(fieldSelect, columns, placeholder, currentField);
        const option = fieldSelect.selectedOptions[0];
        row.dataset.fieldSource = source;
        row.dataset.field = fieldSelect.value;
        row.dataset.fieldType = option?.dataset.type || "";
    });
}

async function refreshRelationBlockOptions(
    block,
    {
        parentTableId = null,
        childTableId = null,
    } = {}
) {
    const options = relationTableOptions();
    const tables = currentTables().filter((table) => table.table);
    const optionValues = new Set(options.map((item) => item.value));
    const parentSelect = block.querySelector('[data-field="parent_table_id"]');
    const childSelect = block.querySelector('[data-field="child_table_id"]');
    const currentParent = parentTableId ?? parentSelect.value;
    const currentChild = childTableId ?? childSelect.value;

    fillObjectSelect(parentSelect, options, "请选择父表", currentParent);
    fillObjectSelect(childSelect, options, "请选择子表", currentChild);

    const nextParent = optionValues.has(currentParent)
        ? currentParent
        : (tables[0]?.id || "");
    const differentChild = tables.find((table) => table.id !== nextParent)?.id || "";
    let nextChild = optionValues.has(currentChild) ? currentChild : "";
    if (!nextChild || (nextChild === nextParent && differentChild)) {
        nextChild = differentChild || nextParent;
    }

    parentSelect.value = nextParent;
    childSelect.value = nextChild;
    await refreshMappingOptions(block);
    await refreshConditionOptions(block);
}

async function refreshRelationOptionsLegacy() {
    const options = relationTableOptions();
    const tables = currentTables().filter((table) => table.table);
    const optionValues = new Set(options.map((item) => item.value));
    for (const block of relationBlocks()) {
        const parentSelect = block.querySelector('[data-field="parent_table_id"]');
        const childSelect = block.querySelector('[data-field="child_table_id"]');
        const currentParent = parentSelect.value;
        const currentChild = childSelect.value;
        fillObjectSelect(parentSelect, options, "请选择父表", currentParent);
        fillObjectSelect(childSelect, options, "请选择子表", currentChild);

        const nextParent = optionValues.has(currentParent)
            ? currentParent
            : (tables[0]?.id || "");
        const differentChild = tables.find((table) => table.id !== nextParent)?.id || "";
        let nextChild = optionValues.has(currentChild) ? currentChild : "";
        if (!nextChild || (nextChild === nextParent && differentChild)) {
            nextChild = differentChild || nextParent;
        }

        parentSelect.value = nextParent;
        childSelect.value = nextChild;
        await refreshMappingOptions(block);
        await refreshConditionOptions(block);
    }
}

async function refreshRelationOptions() {
    for (const block of relationBlocks()) {
        await refreshRelationBlockOptions(block);
    }
}

function addRelationBlock(relation = {}, loadVersion = 0) {
    const template = document.getElementById("relation-template");
    const block = template.content.firstElementChild.cloneNode(true);
    const mappingList = block.querySelector(".mapping-list");
    const conditionList = block.querySelector(".condition-list");
    block.dataset.id = relation.id || makeClientId("rel");
    block.querySelector('[data-field="type"]').value = normalizeRelationType(relation.type);

    block.querySelector(".add-mapping").addEventListener("click", () => {
        addMappingRow(mappingList);
        void refreshMappingOptions(block);
        scheduleSqlPreview();
    });
    block.querySelector(".add-condition").addEventListener("click", () => {
        addConditionRow(conditionList);
        void refreshConditionOptions(block);
        scheduleSqlPreview();
    });
    block.querySelector(".remove-row").addEventListener("click", () => {
        block.remove();
        scheduleSqlPreview();
    });
    block.querySelector('[data-field="parent_table_id"]').addEventListener("change", async () => {
        await refreshMappingOptions(block);
        await refreshConditionOptions(block);
        scheduleSqlPreview();
    });
    block.querySelector('[data-field="child_table_id"]').addEventListener("change", async () => {
        await refreshMappingOptions(block);
        await refreshConditionOptions(block);
        scheduleSqlPreview();
    });
    block.querySelector('[data-field="type"]').addEventListener("change", () => {
        scheduleSqlPreview();
    });

    if (relation.field_pairs?.length) {
        relation.field_pairs.forEach((pair) => addMappingRow(mappingList, pair));
    } else {
        addMappingRow(mappingList);
    }
    (relation.fixed_conditions || []).forEach((condition) => {
        addConditionRow(conditionList, condition);
    });

    relationList.appendChild(block);

    return (async () => {
        await refreshRelationBlockOptions(block, {
            parentTableId: relation.parent_table_id || "",
            childTableId: relation.child_table_id || "",
        });
        if (isStaleFormLoad(loadVersion)) {
            return block;
        }
        relation.field_pairs?.forEach((pair, index) => {
            const row = block.querySelectorAll(".mapping-row")[index];
            if (!row) {
                return;
            }
            row.querySelector('[data-field="parent_field"]').value = pair.parent_field || "";
            row.querySelector('[data-field="child_field"]').value = pair.child_field || "";
        });
        relation.fixed_conditions?.forEach((condition, index) => {
            const row = block.querySelectorAll(".condition-row")[index];
            if (!row) {
                return;
            }
            const source = normalizeConditionSource(condition.source);
            row.querySelector('[data-field="source"]').value = source;
            row.dataset.fieldSource = source;
            updateConditionFieldLabel(row, source);
            row.querySelector('[data-field="field"]').value = condition.field || "";
            row.querySelector('[data-field="operator"]').value = condition.operator || "eq";
            row.querySelector('[data-field="value"]').value = condition.value ?? "";
            const option = row.querySelector('[data-field="field"]').selectedOptions[0];
            row.dataset.field = row.querySelector('[data-field="field"]').value;
            row.dataset.fieldType = option?.dataset.type || "";
        });
        if (!isStaleFormLoad(loadVersion)) {
            scheduleSqlPreview();
        }
        return block;
    })();
}

function gatherJob() {
    const tables = currentTables();
    const job = {
        id: state.editingJobId,
        name: document.getElementById("job-name").value.trim(),
        index_name: document.getElementById("job-index-name").value.trim(),
        enable_assembly_fallback: Boolean(
            assemblyFallbackCheckbox?.checked
        ),
        enable_durable_change_events: Boolean(
            durableChangeEventsCheckbox?.checked
        ),
        poll_interval_seconds: Number(
            document.getElementById("job-poll-interval").value || DEFAULT_POLL_INTERVAL_SECONDS
        ),
        tables,
        relations: relationBlocks().map((block, index) => ({
            id: block.dataset.id || `rel_${index + 1}`,
            parent_table_id: block.querySelector('[data-field="parent_table_id"]').value,
            child_table_id: block.querySelector('[data-field="child_table_id"]').value,
            type: normalizeRelationType(block.querySelector('[data-field="type"]').value),
            field_pairs: [...block.querySelectorAll(".mapping-row")].map((row) => ({
                parent_field: row.querySelector('[data-field="parent_field"]').value.trim(),
                child_field: row.querySelector('[data-field="child_field"]').value.trim(),
            })).filter((pair) => pair.parent_field && pair.child_field),
            fixed_conditions: [...block.querySelectorAll(".condition-row")].map((row) => {
                const source = normalizeConditionSource(
                    row.querySelector('[data-field="source"]').value || row.dataset.fieldSource
                );
                const condition = {
                    field: row.querySelector('[data-field="field"]').value.trim(),
                    operator: row.querySelector('[data-field="operator"]').value,
                    value: parseConditionValue(
                        row.querySelector('[data-field="value"]').value,
                        row.dataset.fieldType || ""
                    ),
                };
                if (source !== "child") {
                    condition.source = source;
                }
                return condition;
            }).filter((condition) => condition.field && condition.value !== ""),
        })),
    };
    if (durableBatchSizeInput?.value) {
        job.durable_change_event_batch_size = Number(durableBatchSizeInput.value);
    }
    if (durableReplayWindowInput?.value) {
        job.durable_change_event_replay_window = Number(durableReplayWindowInput.value);
    }
    if (durableProcessedWindowInput?.value) {
        job.durable_change_event_processed_window = Number(durableProcessedWindowInput.value);
    }
    return job;
}

async function fillJob(job) {
    state.formLoadVersion += 1;
    const loadVersion = state.formLoadVersion;
    const runtime = job?.id ? getJobRuntime(job.id) : {};
    const locked = Boolean(runtime?.job_locked);
    state.editingJobId = job?.id || null;
    document.getElementById("job-name").value = job?.name || "";
    document.getElementById("job-index-name").value = job?.index_name || "";
    if (assemblyFallbackCheckbox) {
        assemblyFallbackCheckbox.checked = Boolean(job?.enable_assembly_fallback);
        assemblyFallbackCheckbox.disabled = !jobUsesCrossDatabase({ tables: job?.tables || [] });
    }
    if (durableChangeEventsCheckbox) {
        durableChangeEventsCheckbox.checked = Boolean(job?.enable_durable_change_events);
    }
    if (durableBatchSizeInput) {
        durableBatchSizeInput.value = job?.durable_change_event_batch_size || "";
    }
    if (durableReplayWindowInput) {
        durableReplayWindowInput.value = job?.durable_change_event_replay_window || "";
    }
    if (durableProcessedWindowInput) {
        durableProcessedWindowInput.value = job?.durable_change_event_processed_window || "";
    }
    document.getElementById("job-poll-interval").value = job?.poll_interval_seconds || DEFAULT_POLL_INTERVAL_SECONDS;
    tableList.innerHTML = "";
    relationList.innerHTML = "";
    const tables = job?.tables || [];
    const relations = job?.relations || [];
    if (tables.length) {
        await Promise.all(tables.map((table) => addTableBlock(table, loadVersion)));
        if (isStaleFormLoad(loadVersion)) {
            return;
        }
    } else {
        await addTableBlock({}, loadVersion);
        if (isStaleFormLoad(loadVersion)) {
            return;
        }
    }
    if (relations.length) {
        await Promise.all(relations.map((relation) => addRelationBlock(relation, loadVersion)));
        if (isStaleFormLoad(loadVersion)) {
            return;
        }
    }
    renderSqlPreviewMessage("配置完任务和关联关系后，这里会自动生成处理 SQL。");
    refreshAssemblyFallbackHint({
        tables,
        enable_assembly_fallback: Boolean(job?.enable_assembly_fallback),
        enable_durable_change_events: Boolean(job?.enable_durable_change_events),
        poll_interval_seconds: job?.poll_interval_seconds,
    });
    await refreshRelationOptions();
    scheduleSqlPreview();
    applyJobEditorLock(locked, job);
}

function renderSqlPreviewMessage(message, isError = false) {
    sqlPreview.innerHTML = "";
    const div = document.createElement("div");
    div.className = isError ? "inline-alert error" : "empty-state";
    div.textContent = message;
    sqlPreview.appendChild(div);
}

function renderSqlPreview(data) {
    sqlPreview.innerHTML = "";
    if (!data?.steps?.length) {
        renderSqlPreviewMessage("当前还没有可展示的 SQL 预览。");
        return;
    }
    const summary = document.createElement("div");
    summary.className = "inline-alert info";
    summary.textContent = `当前任务模式：${localizeMode(data.mode)}。以下 SQL 为方便理解同步逻辑的预览语句。`;
    sqlPreview.appendChild(summary);

    data.steps.forEach((step) => {
        const card = document.createElement("div");
        card.className = "preview-card";
        const meta = [];
        if (step.connection_name) {
            meta.push(`连接：${step.connection_name}`);
        }
        if (step.database) {
            meta.push(`库：${step.database}`);
        }
        if (Object.prototype.hasOwnProperty.call(step, "same_connection")) {
            meta.push(step.same_connection ? "同连接查询" : "跨连接查询");
        }
        card.innerHTML = `
            <strong>${step.title}</strong>
            <div class="preview-meta">${meta.map((item) => `<span>${item}</span>`).join("")}</div>
            <pre class="sql-block"></pre>
        `;
        card.querySelector(".sql-block").textContent = step.sql;
        sqlPreview.appendChild(card);
    });
}

function scheduleSqlPreview() {
    clearTimeout(state.sqlPreviewTimer);
    refreshAssemblyFallbackHint({
        tables: currentTables(),
        enable_assembly_fallback: Boolean(
            jobUsesCrossDatabase({ tables: currentTables() }) && assemblyFallbackCheckbox?.checked
        ),
        enable_durable_change_events: Boolean(durableChangeEventsCheckbox?.checked),
        poll_interval_seconds: document.getElementById("job-poll-interval").value,
    });
    state.sqlPreviewTimer = window.setTimeout(() => {
        void refreshSqlPreview();
    }, 350);
}

async function refreshSqlPreview() {
    const job = gatherJob();
    refreshAssemblyFallbackHint(job);
    const validTables = job.tables.filter((table) => table.connection_id && table.schema && table.table);
    if (!validTables.length) {
        renderSqlPreviewMessage("请先配置至少一张表，随后会自动生成 SQL 预览。");
        return;
    }
    if (job.tables.some((table) => !table.connection_id || !table.schema || !table.table)) {
        renderSqlPreviewMessage("请先补齐所有表的连接、Schema 和表名。");
        return;
    }
    if (job.tables.length > 1 && !job.relations.length) {
        renderSqlPreviewMessage("请先补齐关联关系，随后会自动生成处理 SQL。");
        return;
    }

    const requestId = Date.now();
    state.sqlPreviewRequestId = requestId;
    try {
        const data = await api("/api/jobs/sql-preview", {
            method: "POST",
            body: {
                ...job,
                name: job.name || "__preview__",
                index_name: job.index_name || "__preview_index__",
            },
        });
        if (state.sqlPreviewRequestId !== requestId) {
            return;
        }
        renderSqlPreview(data);
    } catch (error) {
        if (state.sqlPreviewRequestId !== requestId) {
            return;
        }
        renderSqlPreviewMessage(`暂时无法生成 SQL：${error.message}`, true);
    }
}

function getFailureReason(runtime) {
    if (runtime?.last_error) {
        return runtime.last_error;
    }
    const logs = runtime?.logs || [];
    return logs.length ? logs[logs.length - 1] : "未提供失败原因。";
}

function canSearchJob(runtime) {
    return (
        runtime?.status === "running"
        || runtime?.status === "stopped"
        || Boolean(runtime?.last_full_sync_at)
        || Boolean(runtime?.last_incremental_sync_at)
        || Boolean(runtime?.document_count)
        || Number(runtime?.index_document_count || 0) > 0
    );
}

function ensureJobSearchInput(jobId) {
    if (!state.jobSearchInputs[jobId]) {
        state.jobSearchInputs[jobId] = {
            keyword: "",
            size: 10,
            page: 1,
        };
    }
    return state.jobSearchInputs[jobId];
}

function isDeleteJobPromptOpen(jobId) {
    return state.deleteJob.jobId === jobId;
}

function openDeleteJobPrompt(jobId) {
    if (state.deleteJob.jobId !== jobId) {
        state.deleteJob.deleteIndex = false;
    }
    state.deleteJob.jobId = jobId;
    renderJobs();
}

function closeDeleteJobPrompt() {
    state.deleteJob.jobId = "";
    state.deleteJob.deleteIndex = false;
    renderJobs();
}

async function deleteJob(job) {
    const deleteIndex = Boolean(state.deleteJob.deleteIndex);
    const data = await api(`/api/jobs/${job.id}`, {
        method: "DELETE",
        body: {
            delete_index: deleteIndex,
        },
    });
    delete state.jobSearchInputs[job.id];
    delete state.jobSearchResults[job.id];
    state.deleteJob.jobId = "";
    state.deleteJob.deleteIndex = false;
    await loadBootstrap();
    const statusMessage = deleteIndex
        ? `已删除任务：${job.name}，并处理 ES 索引 ${job.index_name}。`
        : `已删除任务：${job.name}，默认保留 ES 索引 ${job.index_name}。`;
    setStatus(statusMessage, data);
}

async function searchJob(jobId) {
    const payload = ensureJobSearchInput(jobId);
    const data = await api(`/api/jobs/${jobId}/search`, {
        method: "POST",
        body: payload,
    });
    state.jobSearchResults[jobId] = data;
    renderJobs();
    setStatus(`已完成任务 ${jobId} 的 ES 查询。`, data);
}

async function runIndexSearch() {
    const jobId = indexSearchJobSelect.value;
    if (!jobId) {
        renderIndexSearchMessage("请先选择一个任务索引。", true);
        switchView("es-query");
        return;
    }

    const rawBody = indexSearchBodyInput.value.trim();
    let body = {};
    if (rawBody) {
        try {
            body = JSON.parse(rawBody);
        } catch (error) {
            renderIndexSearchMessage(`查询条件不是合法 JSON：${error.message}`, true);
            switchView("es-query");
            return;
        }
    }

    state.indexSearch.jobId = jobId;
    state.indexSearch.bodyText = rawBody;
    const data = await api("/api/search/index", {
        method: "POST",
        body: {
            job_id: jobId,
            body,
        },
    });
    state.indexSearchResult = data;
    renderIndexSearchResults(data);
    setStatus(`已完成索引 ${data.job?.index_name || "-"} 的 ES 自定义查询。`, data);
}

function renderIndexAliasMessage(message, isError = false, payload = null) {
    indexAliasResult.innerHTML = "";
    const alert = document.createElement("div");
    alert.className = `inline-alert ${isError ? "error" : "info"}`;
    alert.textContent = message;
    indexAliasResult.appendChild(alert);
    if (payload) {
        const pre = document.createElement("pre");
        pre.className = "result-json";
        pre.textContent = JSON.stringify(payload, null, 2);
        indexAliasResult.appendChild(pre);
    }
}

async function addIndexAlias() {
    const rawBody = indexAliasBodyInput.value.trim();
    let body = {};
    try {
        body = JSON.parse(rawBody);
    } catch (error) {
        try {
            body = JSON.parse(`{${rawBody}}`);
        } catch (wrappedError) {
            renderIndexAliasMessage(
                `Alias JSON 不是合法 JSON：${error.message}`,
                true
            );
            switchView("es-query");
            return;
        }
    }
    const data = await api("/api/search/alias", {
        method: "POST",
        body,
    });
    renderIndexAliasMessage(
        `已为索引 ${data.index} 添加别名 ${data.alias}。`,
        false,
        data
    );
    setStatus(`已为索引 ${data.index} 添加别名 ${data.alias}。`, data);
}

function formatIndexSearchBody() {
    const rawBody = indexSearchBodyInput.value.trim();

    if (!rawBody) {
        state.indexSearch.bodyText = defaultIndexSearchBody();
        indexSearchBodyInput.value = state.indexSearch.bodyText;
        renderIndexSearchMessage("已填充默认 Query DSL，并完成美化。");
        switchView("es-query");
        return;
    }

    try {
        const body = JSON.parse(rawBody);
        state.indexSearch.bodyText = JSON.stringify(body, null, 2);
        indexSearchBodyInput.value = state.indexSearch.bodyText;
        renderIndexSearchMessage("已完成 Query DSL JSON 美化。");
        switchView("es-query");
    } catch (error) {
        renderIndexSearchMessage(`查询条件不是合法 JSON，无法美化：${error.message}`, true);
        switchView("es-query");
    }
}

function buildSearchArea(job, runtime) {
    const container = document.createElement("div");
    container.className = "job-footer";
    const formState = ensureJobSearchInput(job.id);
    const searchPanel = document.createElement("div");
    searchPanel.className = "inline-form";
    searchPanel.innerHTML = `
        <div class="inline-alert info">当前任务状态为 ${localizeStatus(runtime.status)}，可以直接查询索引 ${job.index_name}。</div>
        <div class="grid three">
            <label>查询关键词<input class="search-keyword" placeholder="留空表示 match_all"></label>
            <label>返回条数<input class="search-size" type="number" min="1" max="100"></label>
            <div class="actions">
                <button class="primary search-submit" type="button">查询 ES</button>
            </div>
        </div>
        <div class="result-list"></div>
    `;
    const keywordInput = searchPanel.querySelector(".search-keyword");
    const sizeInput = searchPanel.querySelector(".search-size");
    keywordInput.value = formState.keyword || "";
    sizeInput.value = formState.size || 10;
    keywordInput.addEventListener("input", (event) => {
        formState.keyword = event.target.value;
    });
    sizeInput.addEventListener("input", (event) => {
        formState.size = Number(event.target.value || 10);
    });
    searchPanel.querySelector(".search-submit").addEventListener("click", async () => {
        await searchJob(job.id);
    });

    const resultList = searchPanel.querySelector(".result-list");
    const result = state.jobSearchResults[job.id];
    if (result) {
        const summary = document.createElement("div");
        summary.className = "inline-alert info";
        if (result.exists === false) {
            summary.textContent = `索引 ${job.index_name} 还不存在。`;
            resultList.appendChild(summary);
        } else {
            summary.textContent = `共命中 ${result.total} 条，当前返回 ${result.hits.length} 条，用时 ${result.took} ms。`;
            resultList.appendChild(summary);
            if (!result.hits.length) {
                const empty = document.createElement("div");
                empty.className = "empty-state";
                empty.textContent = "没有查到符合条件的数据。";
                resultList.appendChild(empty);
            }
            result.hits.forEach((hit) => {
                const card = document.createElement("div");
                card.className = "result-card";
                card.innerHTML = `
                    <div class="result-head">
                        <span>文档 ID：${hit.id || "-"}</span>
                        <span>Score：${hit.score ?? "-"}</span>
                    </div>
                    <pre class="result-json"></pre>
                `;
                card.querySelector(".result-json").textContent = JSON.stringify(hit.source || {}, null, 2);
                resultList.appendChild(card);
            });
        }
    }

    container.appendChild(searchPanel);
    return container;
}

function buildFailureArea(runtime) {
    const wrapper = document.createElement("div");
    wrapper.className = "job-footer";
    const alert = document.createElement("div");
    alert.className = "inline-alert error";
    alert.textContent = `失败原因：${getFailureReason(runtime)}`;
    wrapper.appendChild(alert);

    const logs = runtime?.logs || [];
    if (logs.length) {
        const details = document.createElement("details");
        details.innerHTML = "<summary>查看最近日志</summary>";
        const pre = document.createElement("pre");
        pre.className = "result-json";
        pre.textContent = logs.join("\n");
        details.appendChild(pre);
        wrapper.appendChild(details);
    }
    return wrapper;
}

function buildDeleteJobArea(job) {
    const wrapper = document.createElement("div");
    wrapper.className = "job-footer delete-job-area";
    wrapper.innerHTML = `
        <div class="inline-alert error">
            删除任务后，Studio 中的任务配置和运行状态会被移除。默认会保留 ES 索引；如果勾选下面的选项，会同时删除索引 ${job.index_name}。
        </div>
        <label class="checkbox delete-job-checkbox">
            <input class="delete-job-index" type="checkbox">
            <span>同时删除 ES 索引 ${job.index_name}</span>
        </label>
        <div class="actions">
            <button class="danger confirm-delete-job" type="button">确认删除</button>
            <button class="ghost cancel-delete-job" type="button">取消</button>
        </div>
    `;
    const checkbox = wrapper.querySelector(".delete-job-index");
    checkbox.checked = Boolean(state.deleteJob.deleteIndex);
    checkbox.addEventListener("change", (event) => {
        state.deleteJob.deleteIndex = event.target.checked;
    });
    wrapper.querySelector(".confirm-delete-job").addEventListener("click", async () => {
        await runAction(async () => {
            await deleteJob(job);
        }, "删除任务失败");
    });
    wrapper.querySelector(".cancel-delete-job").addEventListener("click", () => {
        closeDeleteJobPrompt();
    });
    return wrapper;
}

function renderJobs() {
    const jobs = state.bootstrap?.jobs || [];
    const runtimes = state.bootstrap?.job_runtime || {};
    jobList.innerHTML = "";
    if (!jobs.length) {
        jobList.innerHTML = "<div class='empty-state'>当前还没有配置同步任务。</div>";
        return;
    }

    jobs.forEach((job) => {
        const runtime = runtimes[job.id] || {};
        const resumeHint = buildResumeHint(runtime);
        const card = document.createElement("div");
        card.className = "row-block";
        card.innerHTML = `
            <div class="job-card">
                <strong>${job.name}</strong>
                <div class="job-meta">
                    <span>索引：${job.index_name}</span>
                    <span>模式：${localizeMode(runtime.mode || "pending")}</span>
                    <span>状态：${localizeStatus(runtime.status || "saved")}</span>
                    <span>健康：${localizeHealthStatus(runtime.health_status || "running")}</span>
                    <span>索引文档数：${runtime.index_document_count ?? 0}</span>
                    <span>同步策略：${crossDatabaseStrategyLabel(job)}</span>
                    <span>${queueMetricText(runtime)}</span>
                    ${lastBatchMetricText(runtime) ? `<span>${lastBatchMetricText(runtime)}</span>` : ""}
                    ${runtime.document_count ? `<span>文档数：${runtime.document_count}</span>` : ""}
                </div>
                <div class="actions">
                    <button class="ghost load-job" type="button">加载</button>
                    <button class="ghost start-job" type="button">启动</button>
                    <button class="ghost stop-job" type="button">停止</button>
                    <button class="danger delete-job" type="button">删除</button>
                </div>
            </div>
        `;

        const meta = card.querySelector(".job-meta");
        const phaseSpan = document.createElement("span");
        phaseSpan.textContent = `阶段：${localizePhase(runtime.phase || "saved")}`;
        meta.appendChild(phaseSpan);
        if (runtime.baseline_completed) {
            const baselineSpan = document.createElement("span");
            baselineSpan.textContent = "基线：已完成";
            meta.appendChild(baselineSpan);
        }
        if (runtime.resume_strategy) {
            const resumeStrategySpan = document.createElement("span");
            resumeStrategySpan.textContent = `恢复策略：${localizeResumeStrategy(runtime.resume_strategy)}`;
            meta.appendChild(resumeStrategySpan);
        }
        if (resumeHint) {
            const hint = document.createElement("div");
            hint.className = "inline-alert info";
            hint.textContent = resumeHint;
            card.querySelector(".job-card").appendChild(hint);
        }

        card.querySelector(".load-job").addEventListener("click", async () => {
            await fillJob(job);
            switchView("tasks");
            setStatus(`已加载任务：${job.name}`, runtime);
        });
        card.querySelector(".start-job").addEventListener("click", async () => {
            const data = await api(`/api/jobs/${job.id}/start`, { method: "POST" });
            await loadBootstrap();
            setStatus(`已启动任务：${job.name}`, data);
        });
        card.querySelector(".stop-job").addEventListener("click", async () => {
            const data = await api(`/api/jobs/${job.id}/stop`, { method: "POST" });
            await loadBootstrap();
            setStatus(`已停止任务：${job.name}`, data);
        });
        card.querySelector(".delete-job").addEventListener("click", async () => {
            await api(`/api/jobs/${job.id}`, { method: "DELETE" });
            delete state.jobSearchInputs[job.id];
            delete state.jobSearchResults[job.id];
            await loadBootstrap();
            setStatus(`已删除任务：${job.name}`);
        });

        if (runtime.status === "failed") {
            card.appendChild(buildFailureArea(runtime));
        } else if (canSearchJob(runtime)) {
            card.appendChild(buildSearchArea(job, runtime));
        }

        jobList.appendChild(card);
    });
}

function renderJobs() {
    const jobs = state.bootstrap?.jobs || [];
    const runtimes = state.bootstrap?.job_runtime || {};
    jobList.innerHTML = "";
    if (!jobs.length) {
        jobList.innerHTML = "<div class='empty-state'>当前还没有配置同步任务。</div>";
        return;
    }

    jobs.forEach((job) => {
        const runtime = runtimes[job.id] || {};
        const resumeHint = buildResumeHint(runtime);
        const card = document.createElement("div");
        card.className = "row-block";
        card.innerHTML = `
            <div class="job-card">
                <strong>${job.name}</strong>
                <div class="job-meta">
                    <span>索引：${job.index_name}</span>
                    <span>模式：${localizeMode(runtime.mode || "pending")}</span>
                    <span>状态：${localizeStatus(runtime.status || "saved")}</span>
                    <span>健康：${localizeHealthStatus(runtime.health_status || "running")}</span>
                    <span>索引文档数：${runtime.index_document_count ?? 0}</span>
                    <span>同步策略：${crossDatabaseStrategyLabel(job)}</span>
                    ${runtime.document_count ? `<span>文档数：${runtime.document_count}</span>` : ""}
                </div>
                <div class="actions">
                    <button class="ghost load-job" type="button">加载</button>
                    <button class="ghost start-job" type="button">启动</button>
                    <button class="ghost stop-job" type="button">停止</button>
                    <button class="danger delete-job" type="button">删除</button>
                </div>
            </div>
        `;

        card.querySelector(".load-job").addEventListener("click", async () => {
            await fillJob(job);
            switchView("tasks");
            setStatus(`已加载任务：${job.name}`, runtime);
        });
        card.querySelector(".start-job").addEventListener("click", async () => {
            const data = await api(`/api/jobs/${job.id}/start`, { method: "POST" });
            await loadBootstrap();
            setStatus(`已启动任务：${job.name}`, data);
        });
        card.querySelector(".stop-job").addEventListener("click", async () => {
            const data = await api(`/api/jobs/${job.id}/stop`, { method: "POST" });
            await loadBootstrap();
            setStatus(`已停止任务：${job.name}`, data);
        });
        const meta = card.querySelector(".job-meta");
        const phaseSpan = document.createElement("span");
        phaseSpan.textContent = `阶段：${localizePhase(runtime.phase || "saved")}`;
        meta.appendChild(phaseSpan);
        if (runtime.baseline_completed) {
            const baselineSpan = document.createElement("span");
            baselineSpan.textContent = "基线：已完成";
            meta.appendChild(baselineSpan);
        }
        if (runtime.resume_strategy) {
            const resumeStrategySpan = document.createElement("span");
            resumeStrategySpan.textContent = `恢复策略：${localizeResumeStrategy(runtime.resume_strategy)}`;
            meta.appendChild(resumeStrategySpan);
        }
        if (resumeHint) {
            const hint = document.createElement("div");
            hint.className = "inline-alert info";
            hint.textContent = resumeHint;
            card.querySelector(".job-card").appendChild(hint);
        }
        card.querySelector(".delete-job").addEventListener("click", () => {
            if (isDeleteJobPromptOpen(job.id)) {
                closeDeleteJobPrompt();
                return;
            }
            openDeleteJobPrompt(job.id);
        });

        if (runtime.status === "failed") {
            card.appendChild(buildFailureArea(runtime));
        } else if (canSearchJob(runtime)) {
            card.appendChild(buildSearchArea(job, runtime));
        }
        if (isDeleteJobPromptOpen(job.id)) {
            card.appendChild(buildDeleteJobArea(job));
        }

        jobList.appendChild(card);
    });
}

function renderJobs() {
    const jobs = getJobs();
    jobList.innerHTML = "";
    if (!jobs.length) {
        jobList.innerHTML = "<div class='empty-state'>当前还没有配置同步任务。</div>";
        return;
    }

    jobs.forEach((job) => {
        const runtime = getJobRuntime(job.id);
        const resumeHint = buildResumeHint(runtime);
        const locked = Boolean(runtime.job_locked);
        const card = document.createElement("div");
        card.className = "row-block";
        card.innerHTML = `
            <div class="job-card">
                <strong>${job.name}</strong>
                <div class="job-meta">
                    <span>索引：${job.index_name}</span>
                    <span>模式：${localizeMode(runtime.mode || "pending")}</span>
                    <span>状态：${localizeStatus(runtime.status || "saved")}</span>
                    <span>健康：${localizeHealthStatus(runtime.health_status || "running")}</span>
                    <span>索引文档数：${runtime.index_document_count ?? 0}</span>
                    <span>编辑：${locked ? "已锁定" : "可修改"}</span>
                    <span>同步策略：${crossDatabaseStrategyLabel(job)}</span>
                    <span>${queueMetricText(runtime)}</span>
                    ${lastBatchMetricText(runtime) ? `<span>${lastBatchMetricText(runtime)}</span>` : ""}
                    ${runtime.document_count ? `<span>文档数：${runtime.document_count}</span>` : ""}
                </div>
                <div class="actions">
                    <button class="ghost load-job" type="button">加载</button>
                    <button class="ghost copy-job" type="button">复制</button>
                    <button class="ghost start-job" type="button">启动</button>
                    <button class="ghost stop-job" type="button">停止</button>
                    <button class="danger delete-job" type="button">删除</button>
                </div>
            </div>
        `;

        const meta = card.querySelector(".job-meta");
        const phaseSpan = document.createElement("span");
        phaseSpan.textContent = `阶段：${localizePhase(runtime.phase || "saved")}`;
        meta.appendChild(phaseSpan);
        if (runtime.baseline_completed) {
            const baselineSpan = document.createElement("span");
            baselineSpan.textContent = "基线：已完成";
            meta.appendChild(baselineSpan);
        }
        if (runtime.resume_strategy) {
            const resumeStrategySpan = document.createElement("span");
            resumeStrategySpan.textContent = `恢复策略：${localizeResumeStrategy(runtime.resume_strategy)}`;
            meta.appendChild(resumeStrategySpan);
        }
        if (resumeHint) {
            const hint = document.createElement("div");
            hint.className = "inline-alert info";
            hint.textContent = resumeHint;
            card.querySelector(".job-card").appendChild(hint);
        }
        if (locked) {
            const hint = document.createElement("div");
            hint.className = "inline-alert info";
            hint.textContent = "当前任务索引已生成，任务内容已锁定。请使用“复制”创建新任务和新索引。";
            card.querySelector(".job-card").appendChild(hint);
        }

        card.querySelector(".load-job").addEventListener("click", async () => {
            await fillJob(job);
            switchView("tasks");
            setStatus(`已加载任务：${job.name}`, runtime);
        });
        card.querySelector(".copy-job").addEventListener("click", () => {
            openCopyJobDialog(job);
        });
        card.querySelector(".start-job").addEventListener("click", async () => {
            const data = await api(`/api/jobs/${job.id}/start`, { method: "POST" });
            await loadBootstrap();
            setStatus(`已启动任务：${job.name}`, data);
        });
        card.querySelector(".stop-job").addEventListener("click", async () => {
            const data = await api(`/api/jobs/${job.id}/stop`, { method: "POST" });
            await loadBootstrap();
            setStatus(`已停止任务：${job.name}`, data);
        });
        card.querySelector(".delete-job").addEventListener("click", () => {
            if (isDeleteJobPromptOpen(job.id)) {
                closeDeleteJobPrompt();
                return;
            }
            openDeleteJobPrompt(job.id);
        });

        if (runtime.status === "failed") {
            card.appendChild(buildFailureArea(runtime));
        } else if (canSearchJob(runtime)) {
            card.appendChild(buildSearchArea(job, runtime));
        }
        if (isDeleteJobPromptOpen(job.id)) {
            card.appendChild(buildDeleteJobArea(job));
        }

        jobList.appendChild(card);
    });
}

async function loadBootstrap() {
    state.bootstrap = await api("/api/bootstrap");
    loadIndexSearchFavorites();
    fillServices(state.bootstrap.services || {});
    renderConnections();
    renderJobs();
    if (!state.indexSearch.bodyText) {
        state.indexSearch.bodyText = defaultIndexSearchBody();
    }
    indexSearchBodyInput.value = state.indexSearch.bodyText;
    fillIndexSearchJobOptions();
    renderIndexSearchFavorites();
    if (
        state.indexSearchResult
        && !getJobs().some((job) => job.id === state.indexSearchResult.job?.id)
    ) {
        state.indexSearchResult = null;
    }
    if (state.indexSearchResult) {
        renderIndexSearchResults(state.indexSearchResult);
    } else if (!getJobs().length) {
        renderIndexSearchMessage("当前还没有可供查询的任务索引，请先创建同步任务。");
    } else {
        renderIndexSearchMessage("请选择任务索引并输入 Elasticsearch Query DSL，随后在这里查看查询结果。");
    }
    tableBlocks().forEach((block) => {
        fillConnectionOptions(
            block.querySelector('[data-field="connection_id"]'),
            block.querySelector('[data-field="connection_id"]').value
        );
    });
    await refreshRelationOptions();
}

menuButtons().forEach((button) => {
    button.addEventListener("click", () => {
        switchView(button.dataset.view);
    });
});

document.getElementById("save-services").addEventListener("click", async () => {
    await runAction(async () => {
        const data = await api("/api/services", {
            method: "POST",
            body: serviceFields(),
        });
        await loadBootstrap();
        setStatus("基础设施配置已保存。", data);
    }, "保存基础设施配置失败");
});

document.getElementById("test-connection").addEventListener("click", async () => {
    await runAction(async () => {
        const data = await api("/api/connections/test", {
            method: "POST",
            body: connectionFields(),
        });
        setStatus("数据库连接测试通过。", data);
    }, "数据库连接测试失败");
});

document.getElementById("save-connection").addEventListener("click", async () => {
    await runAction(async () => {
        const data = await api("/api/connections", {
            method: "POST",
            body: connectionFields(),
        });
        fillConnectionForm(null);
        await loadBootstrap();
        setStatus("数据库连接已保存。", data);
    }, "保存数据库连接失败");
});

document.getElementById("add-table").addEventListener("click", () => {
    addTableBlock();
});

document.getElementById("add-relation").addEventListener("click", () => {
    addRelationBlock();
});

assemblyFallbackCheckbox.addEventListener("change", () => {
    scheduleSqlPreview();
});

durableChangeEventsCheckbox.addEventListener("change", () => {
    scheduleSqlPreview();
});

[durableBatchSizeInput, durableReplayWindowInput, durableProcessedWindowInput]
    .filter(Boolean)
    .forEach((input) => {
        input.addEventListener("input", () => {
            scheduleSqlPreview();
        });
    });

indexSearchJobSelect.addEventListener("change", () => {
    state.indexSearch.jobId = indexSearchJobSelect.value;
    const currentJob = getJobs().find((job) => job.id === state.indexSearch.jobId);
    indexSearchNameInput.value = currentJob?.index_name || "";
    renderIndexSearchFavorites();
});

indexSearchBodyInput.addEventListener("input", () => {
    state.indexSearch.bodyText = indexSearchBodyInput.value;
    state.indexSearch.activeFavoriteId = "";
    renderIndexSearchFavorites();
});

copyJobCancelButton?.addEventListener("click", () => {
    closeCopyJobDialog();
});

copyJobDialog?.addEventListener("cancel", () => {
    resetCopyJobState();
});

copyJobDialog?.addEventListener("close", () => {
    if (!state.copyJob.error) {
        resetCopyJobState();
    }
});

copyJobForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const sourceJobId = state.copyJob.sourceJobId;
    const name = copyJobNameInput.value.trim();
    const indexName = copyJobIndexNameInput.value.trim();
    if (!sourceJobId) {
        setCopyJobError("缺少源任务信息，请关闭后重试。");
        return;
    }
    if (!name) {
        setCopyJobError("任务名称不能为空。");
        return;
    }
    if (!indexName) {
        setCopyJobError("索引名称不能为空。");
        return;
    }
    if (hasJobNameConflict(name)) {
        setCopyJobError(`任务名称 ${name} 已存在，请更换后再创建。`);
        return;
    }
    if (hasJobIndexConflict(indexName)) {
        setCopyJobError(`索引名称 ${indexName} 已存在，请更换后再创建。`);
        return;
    }

    setCopyJobError("");
    try {
        const data = await api(`/api/jobs/${sourceJobId}/copy`, {
            method: "POST",
            body: {
                name,
                index_name: indexName,
            },
        });
        closeCopyJobDialog();
        await loadBootstrap();
        const copiedJob = getJobs().find((job) => job.id === data.job?.id) || data.job;
        await fillJob(copiedJob || null);
        switchView("tasks");
        setStatus(`已创建任务副本：${data.job?.name || name}`, data);
    } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        setCopyJobError(message);
    }
});

saveIndexSearchFavoriteButton.addEventListener("click", () => {
    saveCurrentIndexSearchFavorite();
});

document.getElementById("add-index-alias").addEventListener("click", async () => {
    await runAction(async () => {
        await addIndexAlias();
        switchView("es-query");
    }, "添加 ES 索引别名失败");
});

document.getElementById("run-index-search").addEventListener("click", async () => {
    await runAction(async () => {
        await runIndexSearch();
        switchView("es-query");
    }, "执行 ES 自定义查询失败");
});

document.getElementById("format-index-search").addEventListener("click", () => {
    formatIndexSearchBody();
});

document.getElementById("reset-index-search").addEventListener("click", () => {
    state.indexSearch.bodyText = defaultIndexSearchBody();
    indexSearchBodyInput.value = state.indexSearch.bodyText;
    state.indexSearch.activeFavoriteId = "";
    renderIndexSearchFavorites();
    state.indexSearchResult = null;
    if (getJobs().length) {
        renderIndexSearchMessage("查询条件已重置，请重新执行查询。");
    } else {
        renderIndexSearchMessage("当前还没有可供查询的任务索引，请先创建同步任务。");
    }
});

document.getElementById("validate-job").addEventListener("click", async () => {
    await runAction(async () => {
        const data = await api("/api/jobs/validate", {
            method: "POST",
            body: gatherJob(),
        });
        setStatus("任务校验通过。", data);
        switchView("tasks");
    }, "任务校验失败");
});

document.getElementById("save-job").addEventListener("click", async () => {
    await runAction(async () => {
        const data = await api("/api/jobs", {
            method: "POST",
            body: {
                ...gatherJob(),
                start: true,
            },
        });
        await fillJob(null);
        await loadBootstrap();
        switchView("job-list");
        setStatus("任务已保存并启动。", data);
    }, "保存任务失败");
});

document.getElementById("refresh-jobs").addEventListener("click", async () => {
    await loadBootstrap();
    setStatus("任务列表已刷新。");
});

loadBootstrap()
    .then(async () => {
        fillConnectionForm(null);
        await fillJob(null);
        switchView(state.activeView);
        setStatus("PGSync Studio 已就绪。");
    })
    .catch((error) => {
        setStatus(`初始化失败：${error.message}`);
        switchView("status");
    });
