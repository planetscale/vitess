import React, { useState } from 'react';
import { useHistory, useParams } from 'react-router-dom';
import { deleteTablet, reparentTablet, startReplication, stopReplication } from '../../../api/http';
import { vtadmin } from '../../../proto/vtadmin';
import { isPrimary } from '../../../util/tablets';
import { Icon, Icons } from '../../Icon';
import { success, warn } from '../../Snackbar';
import { TextInput } from '../../TextInput';

interface AdvancedProps {
    tablet: vtadmin.Tablet | undefined;
}

interface RouteParams {
    alias: string;
    clusterID: string;
}

const Advanced: React.FC<AdvancedProps> = ({ tablet }) => {
    const { clusterID, alias } = useParams<RouteParams>();
    const history = useHistory();
    const primary = isPrimary(tablet);
    // DeleteTablet
    const [typedAlias, setTypedAlias] = useState('');
    const [deleteLoading, setDeleteLoading] = useState(false);
    const onDeleteTablet = async () => {
        setDeleteLoading(true);
        let result;
        try {
            result = await deleteTablet({ alias, clusterID });
            if (result) {
                success(`Successfully deleted tablet ${alias}`);
            }
            history.push('/tablets');
        } catch (e) {
            warn(`There was an error deleting tablet: ${e}`);
        }
        setDeleteLoading(false);
    };

    // ReparentTablet
    const [reparentLoading, setReparentLoading] = useState(false);
    const onReparentTablet = async () => {
        setReparentLoading(true);
        let result;
        try {
            result = await reparentTablet({ alias, clusterID });
            if (result) {
                success(`Successfully reparented tablet ${alias} under primary ${result.primary}`, { autoClose: 7000 });
            }
        } catch (e) {
            warn(`There was an error reparenting tablet: ${e}`);
        }
        setReparentLoading(false);
    };

    // StartReplication
    const [startReplicationLoading, setStartReplicationLoading] = useState(false);
    const onStartReplication = async () => {
        setStartReplicationLoading(true);
        let result;
        try {
            result = await startReplication({ alias, clusterID });
            if (result.status === 'ok') {
                success(`Successfully started replication on tablet ${alias}.`, { autoClose: 7000 });
            }
        } catch (e) {
            warn(`There was an error starting replication on tablet: ${e}`);
        }
        setStartReplicationLoading(false);
    };

    // StopReplication
    const [stopReplicationLoading, setStopReplicationLoading] = useState(false);
    const onStopReplication = async () => {
        setStopReplicationLoading(true);
        let result;
        try {
            result = await stopReplication({ alias, clusterID });
            if (result.status === 'ok') {
                success(`Successfully stopped replication on tablet ${alias}.`, { autoClose: 7000 });
            }
        } catch (e) {
            warn(`There was an error stopping replication on tablet: ${e}`);
        }
        setStopReplicationLoading(false);
    };

    return (
        <div className="pt-4">
            <div className="my-8">
                <h3 className="mb-4">Replication</h3>
                <div className="w-full border rounded-lg border-gray-400">
                    <div className="p-8 border-b border-gray-400">
                        <div className="flex justify-between items-center">
                            <p className="text-base font-bold m-0 text-gray-900">Start Replication</p>
                            <a
                                href="https://vitess.io/docs/reference/programs/vtctl/tablets/#startreplication"
                                target="_blank"
                                rel="noreferrer"
                                className="text-gray-900 ml-1"
                            >
                                <span className="text-sm font-semibold text-gray-900">Documentation</span><Icon icon={Icons.open} className="h-6 w-6 ml-1 inline-block text-gray-900 fill-current" />
                            </a>
                        </div>
                        <p className="text-base m-0">
                            This will run the underlying database command to start replication on tablet{' '}
                            <span className="font-bold">{alias}</span>. For example, in mysql 8, this will be{' '}
                            <span className="font-mono text-sm p-1 bg-gray-100">start replication</span>.
                        </p>
                        {primary && (
                            <p className="text-danger">
                                <Icon icon={Icons.alertFail} className="fill-current text-danger inline mr-2" />
                                Command StartTablet cannot be run on the primary tablet.
                            </p>
                        )}
                        <button
                            onClick={onStartReplication}
                            className="btn btn-secondary mt-4"
                            disabled={primary || startReplicationLoading}
                        >
                            Start replication
                        </button>
                    </div>
                    <div className="p-8">
                        <div className="flex justify-between items-center">
                            <p className="text-base font-bold m-0 text-gray-900">Stop Replication</p>
                            <a
                                href="https://vitess.io/docs/reference/programs/vtctl/tablets/#stopreplication"
                                target="_blank"
                                rel="noreferrer"
                                className="text-gray-900"
                            >
                                <span className="text-sm font-semibold text-gray-900">Documentation</span> <Icon icon={Icons.open} className="ml-1 inline-block h-6 w-6 text-gray-900 fill-current" />
                            </a>
                        </div>
                        <p className="text-base m-0">
                            This will run the underlying database command to stop replication on tablet{' '}
                            <span className="font-bold">{alias}</span>. For example, in mysql 8, this will be{' '}
                            <span className="font-mono text-sm p-1 bg-gray-100">stop replication</span>.
                        </p>
                        {primary && (
                            <p className="text-danger">
                                <Icon icon={Icons.alertFail} className="fill-current text-danger inline mr-2" />
                                Command StopTablet cannot be run on the primary tablet.
                            </p>
                        )}
                        <button
                            onClick={onStopReplication}
                            className="btn btn-secondary mt-4"
                            disabled={primary || stopReplicationLoading}
                        >
                            Stop replication
                        </button>
                    </div>
                </div>
            </div>
            <div className="my-8">
                <h3 className="mb-4">Reparent</h3>
                <div className="w-full border rounded-lg border-gray-400">
                    <div className="p-8 border-b border-gray-400">
                        <div className="flex justify-between items-center">
                            <p className="text-base font-bold m-0 text-gray-900">Reparent Tablet</p>
                            <a
                                href="https://vitess.io/docs/reference/programs/vtctl/tablets/#reparenttablet"
                                target="_blank"
                                rel="noreferrer"
                                className="text-gray-900 ml-1"
                            >
                                <span className="text-sm font-semibold text-gray-900">Documentation</span><Icon icon={Icons.open} className="ml-1 h-6 w-6 inline-block text-gray-900 fill-current" />
                            </a>
                        </div>
                        <p className="text-base m-0">
                            Reconnect replication for tablet <span className="font-bold">{alias}</span> to the current
                            primary tablet. This only works if the current replication position matches the last known
                            reparent action.
                        </p>
                        {primary && (
                            <p className="text-danger">
                                <Icon icon={Icons.alertFail} className="fill-current text-danger inline mr-2" />
                                Command ReparentTablet cannot be run on the primary tablet.
                            </p>
                        )}
                        <button
                            className="btn btn-secondary mt-4"
                            disabled={primary || reparentLoading}
                            onClick={onReparentTablet}
                        >
                            Reparent tablet
                        </button>
                    </div>
                </div>
            </div>
            <div className="my-8">
                <h3 className="mb-4">Danger</h3>
                <div className="border border-danger rounded-lg p-8">
                    <div className="flex justify-between items-center">
                        <p className="text-base font-bold m-0 text-gray-900">Delete Tablet</p>
                        <a
                            href="https://vitess.io/docs/reference/programs/vtctl/tablets/#deletetablet"
                            target="_blank"
                            rel="noreferrer"
                            className="text-gray-900 ml-1 inline-block"
                        >
                            <span className="text-sm font-semibold text-gray-900">Documentation</span><Icon icon={Icons.open} className="ml-1 h-6 w-6 text-gray-900 fill-current inline" />
                        </a>
                    </div>
                    <p className="text-base mt-0">
                        Delete tablet <span className="font-bold">{alias}</span>. Doing so will remove it from the
                        topology, but vttablet and MySQL won't be touched.
                    </p>
                    {primary && (
                        <p className="text-danger">
                            <Icon icon={Icons.alertFail} className="fill-current text-danger inline mr-2" />
                            Tablet {alias} is the primary tablet. Flag{' '}
                            <span className="font-mono bg-red-100 p-1 text-sm">-allow_master=true</span> will be applied
                            in order to delete the primary tablet.
                        </p>
                    )}

                    <p className="text-base">Please type the tablet's alias to delete the tablet:</p>
                    <div className="w-1/3">
                        <TextInput
                            placeholder="zone-xxx"
                            value={typedAlias}
                            onChange={(e) => setTypedAlias(e.target.value)}
                        />
                    </div>
                    <button
                        className="btn btn-secondary btn-danger mt-4"
                        disabled={typedAlias !== alias || deleteLoading}
                        onClick={onDeleteTablet}
                    >
                        {deleteLoading ? 'Deleting...' : 'Delete'}
                    </button>
                </div>
            </div>
        </div >
    );
};

export default Advanced;
