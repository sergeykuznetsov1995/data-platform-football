from __future__ import annotations

import json
import hashlib
from pathlib import Path

import pytest

from deploy.shared_writer import processes


CID = 'a' * 64


@pytest.fixture
def host(tmp_path, monkeypatch):
    proc = tmp_path / 'proc'
    (proc / 'self/ns').mkdir(parents=True)
    (proc / 'self/ns/mnt').write_text('host namespace')
    (proc / 'self/ns/pid').write_text('host pid namespace')
    tail = tmp_path / 'tail'
    tail.write_bytes(b'trusted tail binary')
    tail.chmod(0o755)
    monkeypatch.setattr(processes, 'PROC_ROOT', proc)
    monkeypatch.setattr(processes, 'TAIL', tail)
    shared = tmp_path / 'shared'
    (shared / 'scrapers/base').mkdir(parents=True)
    (shared / 'scrapers/base/iceberg_writer.py').write_text('shared')
    (shared / 'dags').mkdir()
    (shared / 'scripts').mkdir()
    separate = tmp_path / 'separate'
    (separate / 'base').mkdir(parents=True)
    (separate / 'base/iceberg_writer.py').write_text('separate')
    rootfs = tmp_path / 'rootfs'
    observed = rootfs / 'opt/airflow/scrapers/base/iceberg_writer.py'
    observed.parent.mkdir(parents=True)
    observed.hardlink_to(separate / 'base/iceberg_writer.py')
    namespace = tmp_path / 'container-mnt'
    namespace.write_text('container namespace')
    binaries = tmp_path / 'bin'
    binaries.mkdir()
    for name in ('python3.11', 'bash', 'tini', 'tail'):
        (binaries / name).write_text(name)
    def add(pid, executable='python3.11', cgroup=f'0::/system.slice/docker-{CID}.scope', root=rootfs):
        directory = proc / str(pid)
        (directory / 'ns').mkdir(parents=True)
        (directory / 'stat').write_text(f'{pid} (process ) name) S ' + '0 ' * 18 + '100 0 0')
        (directory / 'cgroup').write_text(cgroup)
        (directory / 'root').symlink_to(root, target_is_directory=True)
        (directory / 'exe').symlink_to(binaries / executable)
        (directory / 'ns/mnt').symlink_to(namespace)
        (directory / 'ns/pid').symlink_to(namespace)
        return directory
    add(10)
    add(11, 'tini')
    row = {'id': CID, 'status': 'running', 'init_pid': 11, 'privileged': False,
           'pid_mode': '', 'cap_add': None,
           'mounts': [{'Type': 'bind', 'Source': str(separate), 'Destination': '/opt/airflow/scrapers', 'RW': False}]}
    return {'proc': proc, 'tail': tail, 'shared': shared, 'separate': separate,
            'rootfs': rootfs, 'row': row, 'add': add, 'binaries': binaries}


def classify(host, pids=None, rows=None):
    return processes.classify_processes(pids or [10], rows if rows is not None else [host['row']], host['shared'])


def test_separate_source_requires_actual_container_and_writer_identity(host):
    result = classify(host)[0]
    assert result == {'pid': 10, 'blocks': False, 'reason': 'separate_container_writer',
                      'start_time': 100, 'container_id': CID}


@pytest.mark.parametrize('cgroup', [f'0::/docker/{CID}', f'1:cpu:/docker/{CID}\n2:memory:/docker/{CID}'])
def test_cgroup_v1_and_v2_full_container_ids(host, cgroup):
    for pid in (10, 11):
        (host['proc'] / str(pid) / 'cgroup').write_text(cgroup)
    assert classify(host)[0]['blocks'] is False


@pytest.mark.parametrize('cgroup', ['0::/user.slice', f'0::/docker/{CID[:12]}', f'0::/not-docker-{CID}.scope',
                                   f'0::/docker/{CID}\n1:cpu:/docker/' + 'b' * 64])
def test_unknown_or_ambiguous_container_blocks(host, cgroup):
    (host['proc'] / '10/cgroup').write_text(cgroup)
    assert classify(host)[0]['blocks'] is True


@pytest.mark.parametrize(('field', 'value'), [('status', 'exited'), ('privileged', True),
    ('pid_mode', 'host'), ('cap_add', ['SYS_ADMIN']), ('init_pid', 999)])
def test_insufficient_container_isolation_blocks(host, field, value):
    host['row'][field] = value
    assert classify(host)[0]['blocks'] is True


@pytest.mark.parametrize('field', ['privileged', 'pid_mode', 'init_pid', 'mounts', 'cap_add'])
def test_missing_container_evidence_blocks(host, field):
    del host['row'][field]
    assert classify(host)[0]['blocks'] is True


def test_names_and_process_titles_do_not_exclude_shell_deliveries(host):
    exe = host['proc'] / '10/exe'
    exe.unlink()
    exe.symlink_to(host['binaries'] / 'bash')
    assert classify(host)[0]['blocks'] is True


def test_forged_tail_executable_name_is_not_exempt(host):
    exe = host['proc'] / '10/exe'
    exe.unlink()
    exe.symlink_to(host['binaries'] / 'tail')
    assert classify(host)[0]['blocks'] is True


@pytest.mark.parametrize('copied', [False, True])
def test_tail_requires_trusted_inode_or_identical_binary(host, copied):
    target = host['tail']
    if copied:
        target = host['binaries'] / 'tail (deleted)'
        target.write_bytes(host['tail'].read_bytes())
    exe = host['proc'] / '10/exe'
    exe.unlink()
    exe.symlink_to(target)
    assert classify(host)[0]['reason'] == 'verified_tail'
    assert classify(host)[0]['blocks'] is False


def test_mutable_trusted_tail_does_not_authorize_exemption(host):
    host['tail'].chmod(0o777)
    exe = host['proc'] / '10/exe'
    exe.unlink()
    exe.symlink_to(host['tail'])
    assert classify(host)[0]['blocks'] is True


@pytest.mark.parametrize('source_kind', ['direct', 'symlink', 'hardlink'])
def test_shared_writer_aliases_block(host, source_kind):
    shared_writer = host['shared'] / 'scrapers/base/iceberg_writer.py'
    other_writer = host['separate'] / 'base/iceberg_writer.py'
    observed = host['rootfs'] / processes.WRITER.relative_to('/')
    observed.unlink()
    if source_kind == 'direct':
        host['row']['mounts'][0]['Source'] = str(host['shared'] / 'scrapers')
        observed.hardlink_to(shared_writer)
    else:
        other_writer.unlink()
        if source_kind == 'symlink':
            other_writer.symlink_to(shared_writer)
        else:
            other_writer.hardlink_to(shared_writer)
        observed.hardlink_to(shared_writer)
    assert classify(host)[0]['blocks'] is True


def test_actual_writer_must_match_docker_mount_source(host):
    observed = host['rootfs'] / processes.WRITER.relative_to('/')
    observed.unlink()
    observed.write_text('unreported replacement')
    assert classify(host)[0]['blocks'] is True


@pytest.mark.parametrize('relative', ['.', 'scrapers', 'scrapers/base'])
def test_additional_bind_alias_of_writer_ancestor_blocks(host, monkeypatch, relative):
    alias = host['shared'].parent / 'alternate-base'
    alias.mkdir()
    shared_directory = host['shared'] / relative
    original_identity = processes._identity

    def bind_identity(path):
        # A real bind mount keeps its source inode, but realpath still returns
        # the alias path. Model that filesystem property without live mounts.
        return original_identity(shared_directory if path == alias else path)

    monkeypatch.setattr(processes, '_identity', bind_identity)
    host['row']['mounts'].append({
        'Type': 'bind', 'Source': str(alias), 'Destination': '/alternate-base',
    })
    assert classify(host)[0]['blocks'] is True


def test_additional_hardlink_of_shared_writer_blocks(host):
    alias = host['shared'].parent / 'alternate-writer.py'
    alias.hardlink_to(host['shared'] / 'scrapers/base/iceberg_writer.py')
    host['row']['mounts'].append({
        'Type': 'bind', 'Source': str(alias), 'Destination': '/alternate-writer.py',
    })
    assert classify(host)[0]['blocks'] is True


@pytest.mark.parametrize('source', ['shared_parent', 'socket', 'run'])
def test_broad_shared_mounts_and_engine_access_block(host, source):
    path = {'shared_parent': host['shared'].parent, 'run': Path('/run')}.get(source)
    if source == 'socket':
        path = host['shared'].parent / 'docker.sock'
        path.write_text('socket stand-in')
    host['row']['mounts'].append({'Type': 'bind', 'Source': str(path), 'Destination': '/extra'})
    assert classify(host)[0]['blocks'] is True


def test_matching_cgroup_with_different_root_or_mount_namespace_blocks(host):
    root = host['proc'] / '10/root'
    root.unlink()
    root.symlink_to(host['shared'])
    assert classify(host)[0]['blocks'] is True


def test_different_mount_namespace_blocks(host):
    ns = host['proc'] / '10/ns/mnt'
    ns.unlink()
    ns.write_text('other namespace')
    assert classify(host)[0]['blocks'] is True


def test_reused_pid_or_exec_during_observation_blocks(host, monkeypatch):
    original = processes._separate_writer
    def changing(*args):
        allowed = original(*args)
        path = host['proc'] / '10/stat'
        path.write_text(path.read_text().replace('100 0 0', '101 0 0'))
        return allowed
    monkeypatch.setattr(processes, '_separate_writer', changing)
    result = classify(host)[0]
    assert result['reason'] == 'process_changed'
    assert result['blocks'] is True


def test_disappeared_pid_is_nonblocking_but_partial_proc_read_blocks(host):
    assert classify(host, [999])[0] == {'pid': 999, 'blocks': False, 'reason': 'exited'}
    (host['proc'] / '10/cgroup').unlink()
    assert classify(host)[0]['blocks'] is True


def test_evidence_never_copies_cmdline_cgroup_or_executable_strings(host):
    directory = host['proc'] / '10'
    secret = 'secret-token-do-not-report'
    (directory / 'cmdline').write_text(secret)
    (directory / 'cgroup').write_text('0::/' + secret)
    evidence = json.dumps(classify(host))
    assert secret not in evidence
    assert 'cmdline' not in evidence
    assert 'cgroup' not in evidence


def test_ambiguous_inventory_and_empty_inventory_block(host):
    assert classify(host, rows=[])[0]['blocks'] is True
    assert classify(host, rows=[host['row'], host['row']])[0]['blocks'] is True


def test_candidate_pids_are_deduplicated_and_validated(host):
    assert len(classify(host, [10, 10])) == 1
    with pytest.raises(ValueError, match='invalid driver PID'):
        classify(host, [True])


def test_oversized_executable_is_not_hashed_without_bound(host, monkeypatch):
    monkeypatch.setattr(processes, 'MAX_BINARY_BYTES', 4)
    assert classify(host)[0]['blocks'] is True


def test_tail_permission_error_stays_blocking_and_sanitized(host, monkeypatch):
    def unreadable(*args):
        raise PermissionError('sensitive path or value')
    monkeypatch.setattr(processes, '_trusted_tail', unreadable)
    result = classify(host)[0]
    assert result['blocks'] is True
    assert 'sensitive' not in json.dumps(result)


def test_missing_observed_writer_blocks(host):
    (host['rootfs'] / processes.WRITER.relative_to('/')).unlink()
    assert classify(host)[0]['blocks'] is True


def test_container_anchor_cgroup_must_also_match(host):
    (host['proc'] / '11/cgroup').write_text('0::/docker/' + 'b' * 64)
    assert classify(host)[0]['blocks'] is True


def test_host_namespace_is_never_isolated_by_container_label(host):
    for pid in (10, 11):
        ns = host['proc'] / str(pid) / 'ns/mnt'
        ns.unlink()
        ns.symlink_to(host['proc'] / 'self/ns/mnt')
    assert classify(host)[0]['blocks'] is True


def test_init_pid_reuse_during_observation_blocks(host, monkeypatch):
    original = processes._process
    calls = 0
    def changing(pid):
        nonlocal calls
        if pid == 11:
            calls += 1
            if calls == 2:
                path = host['proc'] / '11/stat'
                path.write_text(path.read_text().replace('100 0 0', '101 0 0'))
        return original(pid)
    monkeypatch.setattr(processes, '_process', changing)
    assert classify(host)[0]['blocks'] is True


def test_known_capabilities_are_safe_only_with_private_pid_namespace(host):
    host['row']['cap_add'] = ['CAP_CHOWN', 'CAP_SETUID', 'CAP_SETGID', 'CAP_SYS_PTRACE']
    assert classify(host)[0]['blocks'] is False
    for pid in (10, 11):
        ns = host['proc'] / str(pid) / 'ns/pid'
        ns.unlink()
        ns.symlink_to(host['proc'] / 'self/ns/pid')
    assert classify(host)[0]['blocks'] is True


def test_unrelated_config_mount_inside_shared_root_is_not_writer_access(host):
    config = host['shared'] / 'proxy-list.txt'
    config.write_text('not read by classifier')
    host['row']['mounts'].append({'Type': 'bind', 'Source': str(config), 'Destination': '/config.txt'})
    assert classify(host)[0]['blocks'] is False


def test_unknown_added_capability_blocks(host):
    host['row']['cap_add'] = ['ALL']
    assert classify(host)[0]['blocks'] is True


def test_verified_historical_package_binary_survives_upgrade(host, monkeypatch):
    previous = host['binaries'] / 'tail (deleted)'
    previous.write_bytes(b'verified historic package binary')
    monkeypatch.setattr(processes, '_PACKAGED_TAIL_SHA256', frozenset({hashlib.sha256(previous.read_bytes()).hexdigest()}))
    exe = host['proc'] / '10/exe'
    exe.unlink()
    exe.symlink_to(previous)
    assert classify(host)[0]['blocks'] is False
    previous.write_bytes(b'changed binary with same tail name')
    assert classify(host)[0]['blocks'] is True
