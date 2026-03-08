import type { DatasetCatalogItem } from '@/orchestration/datasets/types';

export interface FederatedBindingDirective {
  alias: string;
  datasetId: string;
}

const DIRECTIVE_PREFIX = '-- langbridge:federated-source';

export function supportsStructuredFederatedDataset(dataset: DatasetCatalogItem): boolean {
  return Boolean(
    dataset.storageKind !== 'virtual'
    && dataset.datasetType !== 'FEDERATED'
    &&
    dataset.executionCapabilities?.supportsStructuredScan
    && dataset.executionCapabilities?.supportsSqlFederation,
  );
}

export function shouldDefaultFederatedMode(options: {
  allowFederation?: boolean;
  datasets: DatasetCatalogItem[];
}): boolean {
  if (!options.allowFederation) {
    return false;
  }
  return options.datasets.some((dataset) => supportsStructuredFederatedDataset(dataset));
}

export function buildFederatedDirectiveLines(bindings: FederatedBindingDirective[]): string {
  return bindings
    .map((binding) => {
      const alias = String(binding.alias || '').trim();
      const datasetId = String(binding.datasetId || '').trim();
      if (!alias || !datasetId) {
        return null;
      }
      return `${DIRECTIVE_PREFIX} alias=${alias} dataset_id=${datasetId}`;
    })
    .filter((line): line is string => Boolean(line))
    .join('\n');
}

export function parseFederatedDirectiveBindings(queryText: string): FederatedBindingDirective[] {
  const results: FederatedBindingDirective[] = [];
  for (const line of String(queryText || '').split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed.startsWith(DIRECTIVE_PREFIX)) {
      continue;
    }
    const payload = trimmed.slice(DIRECTIVE_PREFIX.length).trim();
    const fields = Object.fromEntries(
      payload
        .split(/\s+/)
        .map((part) => part.split('='))
        .filter((entry): entry is [string, string] => entry.length === 2 && Boolean(entry[0])),
    );
    const alias = String(fields.alias || '').trim();
    const datasetId = String(fields.dataset_id || '').trim();
    if (!alias || !datasetId) {
      continue;
    }
    results.push({
      alias,
      datasetId,
    });
  }
  return results;
}

export function stripFederatedDirectiveBindings(queryText: string): string {
  return String(queryText || '')
    .split(/\r?\n/)
    .filter((line) => !line.trim().startsWith(DIRECTIVE_PREFIX))
    .join('\n')
    .replace(/^\s+/, '');
}

export function injectFederatedDirectives(
  queryText: string,
  bindings: FederatedBindingDirective[],
): string {
  const directiveText = buildFederatedDirectiveLines(bindings);
  const strippedQuery = stripFederatedDirectiveBindings(queryText).trim();
  if (!directiveText) {
    return strippedQuery;
  }
  if (!strippedQuery) {
    return directiveText;
  }
  return `${directiveText}\n${strippedQuery}`;
}
