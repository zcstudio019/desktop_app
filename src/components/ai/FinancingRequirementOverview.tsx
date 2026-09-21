import { useEffect, useState } from 'react';
import { getCurrentFinancingRequirement, getPendingFinancingRequirement, type FinancingRequirementData } from '../../services/api';
import { useChatStore } from '../../stores/useChatStore';
import { FinancingRequirementCard } from '../FinancingRequirementCard';

export default function FinancingRequirementOverview() {
  const { aiContext } = useChatStore();
  const customerId = aiContext.selectedCustomer?.id;
  const [current, setCurrent] = useState<FinancingRequirementData | null>(null);
  useEffect(() => {
    let active = true;
    async function refresh() {
      if (!customerId) { setCurrent(null); return; }
      try {
        const requirement = await getPendingFinancingRequirement(customerId) || await getCurrentFinancingRequirement(customerId);
        if (active) setCurrent(requirement);
      } catch { if (active) setCurrent(null); }
    }
    void refresh();
    window.addEventListener('financing-requirement-updated', refresh);
    return () => { active = false; window.removeEventListener('financing-requirement-updated', refresh); };
  }, [customerId]);
  if (!customerId) return null;
  return <div className="border-b border-slate-100 px-3 py-2" data-testid="financing-requirement-overview">
    {current ? <FinancingRequirementCard initial={current} /> : <span className="text-xs text-slate-500">当前尚未确认融资需求</span>}
  </div>;
}
