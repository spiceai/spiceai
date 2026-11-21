// Copyright 2024-2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// An in-memory stand-in for the parts of the Octokit client that
// actions/github-script passes as `github`, for test_pure_revert.mjs and
// test_trunk_health.mjs. It answers from plain data and records every write.

const httpError = (status, message) => Object.assign(new Error(message), { status });

/**
 * `state` fields, all optional:
 * - issues: [{ number, title, labels: [name], pr: bool }] — open issues and PRs
 * - events: { [number]: [{ event, label, created_at }] }
 * - runs: [{ conclusion, head_sha }] newest first, or 'missing' for a 404
 * - pulls: { [number]: { labels: [name] } }
 * - closing: { [number]: [{ number, labels: [name], repo? }] } (repo defaults to 'o/r')
 * - commits: { [number]: [{ sha, message }] }
 * - onBase: Set of SHAs that are ancestors of the base
 * - comments: { [number]: [{ id, body, login }] }
 * - queue: [{ sha?, message }] — a merge group's commits not yet on its branch
 * - fail: { [method]: Error } — that call throws
 */
export function fakeGithub(state = {}) {
  const writes = [];
  const calls = [];
  const maybeFail = (method) => {
    calls.push(method);
    if (state.fail?.[method]) throw state.fail[method];
  };
  const comments = state.comments ?? {};
  let nextComment = 1000;

  const github = {
    paginate: async (method, params) => (await method(params)).data,
    // Pages closing references 100 at a time, as `first: 100` asks.
    graphql: async (_query, { number, cursor }) => {
      maybeFail('graphql');
      const all = state.closing?.[number] ?? [];
      const start = cursor === null || cursor === undefined ? 0 : Number(cursor);
      const end = Math.min(start + 100, all.length);
      return {
        repository: {
          pullRequest: {
            closingIssuesReferences: {
              pageInfo: { hasNextPage: end < all.length, endCursor: String(end) },
              nodes: all.slice(start, end).map((issue) => ({
                number: issue.number,
                repository: { nameWithOwner: issue.repo ?? 'o/r' },
                labels: { nodes: issue.labels.map((name) => ({ name })) },
              })),
            },
          },
        },
      };
    },
    rest: {
      issues: {
        listForRepo: async ({ labels, state: wanted }) => {
          maybeFail('issues.listForRepo');
          if (wanted !== 'open') throw new Error('only open issues are modelled');
          return {
            data: (state.issues ?? [])
              .filter((issue) => issue.labels.includes(labels))
              .map((issue) => ({
                number: issue.number,
                title: issue.title ?? `item ${issue.number}`,
                html_url: `https://github.com/o/r/issues/${issue.number}`,
                ...(issue.pr ? { pull_request: {} } : {}),
              })),
          };
        },
        listEvents: async ({ issue_number }) => {
          maybeFail('issues.listEvents');
          return {
            data: (state.events?.[issue_number] ?? []).map((e) => ({
              event: e.event,
              label: { name: e.label },
              created_at: e.created_at,
            })),
          };
        },
        listComments: async ({ issue_number }) => {
          maybeFail('issues.listComments');
          return {
            data: (comments[issue_number] ?? []).map((c) => ({
              id: c.id,
              body: c.body,
              user: { login: c.login },
            })),
          };
        },
        createComment: async ({ issue_number, body }) => {
          maybeFail('issues.createComment');
          writes.push({ op: 'create', issue_number, body });
          (comments[issue_number] ??= []).push({ id: nextComment++, body, login: 'github-actions[bot]' });
          return { data: {} };
        },
        updateComment: async ({ comment_id, body }) => {
          maybeFail('issues.updateComment');
          writes.push({ op: 'update', comment_id, body });
          return { data: {} };
        },
      },
      actions: {
        // Honours page and per_page, as the API does.
        listWorkflowRuns: async ({ workflow_id, status, page = 1, per_page = 30 }) => {
          maybeFail('actions.listWorkflowRuns');
          calls.push(`actions.listWorkflowRuns page ${page}`);
          if (state.runs === 'missing') throw httpError(404, `Not Found: ${workflow_id}`);
          if (status !== 'completed') throw new Error('only completed runs are modelled');
          const start = (page - 1) * per_page;
          return {
            data: {
              workflow_runs: (state.runs ?? []).slice(start, start + per_page).map((run, i) => ({
                html_url: `https://github.com/o/r/actions/runs/${start + i}`,
                head_sha: run.head_sha ?? `${start + i}`.padStart(40, '0'),
                conclusion: run.conclusion,
              })),
            },
          };
        },
      },
      pulls: {
        get: async ({ pull_number }) => {
          maybeFail('pulls.get');
          const pr = state.pulls?.[pull_number] ?? { labels: [] };
          return { data: { number: pull_number, labels: pr.labels.map((name) => ({ name })) } };
        },
        listCommits: async ({ pull_number }) => {
          maybeFail('pulls.listCommits');
          return {
            data: (state.commits?.[pull_number] ?? []).map((c) => ({
              sha: c.sha,
              commit: { message: c.message },
            })),
          };
        },
      },
      repos: {
        // `<branch>...<group head>` lists the merge group's commits not yet on
        // the branch (state.queue, default: one commit for PR 42); `<sha>...<sha>`
        // answers the pure-revert ancestry check from state.onBase.
        compareCommitsWithBasehead: async ({ basehead }) => {
          maybeFail('repos.compareCommitsWithBasehead');
          const [base, head] = basehead.split('...');
          if (!/^[0-9a-f]{40}$/.test(base)) {
            const queue = state.queue ?? [{ sha: head, message: 'feat: something (#42)' }];
            return {
              data: { commits: queue.map((c) => ({ sha: c.sha ?? head, commit: { message: c.message } })) },
            };
          }
          return { data: { ahead_by: state.onBase?.has(head) ? 0 : 1 } };
        },
      },
    },
  };
  return { github, writes, calls };
}

/** Records what a script did, in place of the Actions toolkit's `core`. */
export function fakeCore() {
  const out = { failed: [], warnings: [], notices: [], infos: [], summary: '' };
  const core = {
    setFailed: (m) => out.failed.push(m),
    warning: (m) => out.warnings.push(m),
    notice: (m) => out.notices.push(m),
    info: (m) => out.infos.push(m),
    summary: {
      addRaw(text) {
        out.summary += text;
        return this;
      },
      write: async () => {},
    },
  };
  return { core, out };
}

/** A minimal test runner: register with `test`, then `await runAll()`. */
export function testRunner() {
  const tests = [];
  return {
    test: (name, body) => tests.push({ name, body }),
    runAll: async () => {
      let failures = 0;
      for (const { name, body } of tests) {
        try {
          // eslint-disable-next-line no-await-in-loop
          await body();
          console.log(`ok   ${name}`);
        } catch (error) {
          failures += 1;
          console.error(`FAIL ${name}\n     ${error.message}`);
        }
      }
      console.log(`\n${tests.length - failures}/${tests.length} passed`);
      process.exit(failures === 0 ? 0 : 1);
    },
  };
}
