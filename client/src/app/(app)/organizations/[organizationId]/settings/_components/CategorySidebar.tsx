'use client';

import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/utils';
import { ALL_CATEGORIES, type SettingsCategory } from '../_lib/settings-utils';

export type CategoryCount = {
  category: SettingsCategory;
  count: number;
};

interface CategorySidebarProps {
  categories: CategoryCount[];
  selectedCategory: SettingsCategory | typeof ALL_CATEGORIES;
  onSelectCategory: (category: SettingsCategory | typeof ALL_CATEGORIES) => void;
}

export function CategorySidebar({
  categories,
  selectedCategory,
  onSelectCategory,
}: CategorySidebarProps) {
  const totalCount = categories.reduce((acc, item) => acc + item.count, 0);

  return (
    <aside className="surface-panel rounded-3xl p-4 shadow-soft">
      <div className="mb-3 flex items-center justify-between">
        <p className="text-xs font-semibold uppercase tracking-[0.18em] text-[color:var(--text-muted)]">
          Categories
        </p>
        <Badge variant="secondary">{totalCount}</Badge>
      </div>

      <div className="space-y-2">
        <button
          type="button"
          className={cn(
            'flex w-full items-center justify-between rounded-2xl border px-3 py-2 text-left text-sm transition',
            selectedCategory === ALL_CATEGORIES
              ? 'border-[color:var(--accent)] bg-[color:var(--accent-soft)] text-[color:var(--text-primary)]'
              : 'border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] text-[color:var(--text-secondary)] hover:border-[color:var(--border-strong)] hover:text-[color:var(--text-primary)]',
          )}
          onClick={() => onSelectCategory(ALL_CATEGORIES)}
        >
          <span>{ALL_CATEGORIES}</span>
          <Badge variant="secondary">{totalCount}</Badge>
        </button>

        {categories.map((item) => (
          <button
            key={item.category}
            type="button"
            className={cn(
              'flex w-full items-center justify-between rounded-2xl border px-3 py-2 text-left text-sm transition',
              selectedCategory === item.category
                ? 'border-[color:var(--accent)] bg-[color:var(--accent-soft)] text-[color:var(--text-primary)]'
                : 'border-[color:var(--panel-border)] bg-[color:var(--panel-bg)] text-[color:var(--text-secondary)] hover:border-[color:var(--border-strong)] hover:text-[color:var(--text-primary)]',
            )}
            onClick={() => onSelectCategory(item.category)}
          >
            <span>{item.category}</span>
            <Badge variant="secondary">{item.count}</Badge>
          </button>
        ))}
      </div>
    </aside>
  );
}
