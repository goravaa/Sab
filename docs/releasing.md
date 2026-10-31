# Releasing Sab images

The `v*.*.*` tag workflow builds both Docker images for amd64 and arm64,
publishes version tags (for example, `0.1.0`) and `latest` to Docker Hub, and
creates a GitHub release using `RELEASE_NOTES.md`. A manual dispatch can
republish a version by supplying its number without the `v` prefix.

## One-time GitHub setup

The workflow does not contain Docker Hub credentials. It reads these Actions
secrets when it logs in:

- `DOCKERHUB_USERNAME`: the Docker Hub account with push access to the
  `garvw/sab-dataset-engine` and `garvw/sab-query-service` repositories
  (currently `garvw`).
- `DOCKERHUB_TOKEN`: a Docker Hub access token for that account, with
  permission to push to both repositories.

Create the token in Docker Hub's account security settings, then add both
values under the GitHub repository's **Settings → Secrets and variables →
Actions → Repository secrets**. Store the token as `DOCKERHUB_TOKEN`; do not
commit it or put it in Compose environment files. GitHub masks Actions secrets
in workflow logs.

## Publish or retry a release

Once the secrets are configured, pushing a version tag starts publication:

```sh
git tag -a v0.1.0 -m "Sab v0.1.0"
git push origin v0.1.0
```

To retry the existing `v0.1.0` release after configuring secrets, use **Actions
→ Release Docker images → Run workflow** and enter `0.1.0`. The manual run
builds the current default branch; use it to retry a release only when that
branch contains the source intended for the version. Check the run result
before using the published tags.
