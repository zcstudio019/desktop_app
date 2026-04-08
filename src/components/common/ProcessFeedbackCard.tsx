import React from 'react';
import { AlertCircle, AlertTriangle, CheckCircle2, Loader2, Sparkles } from 'lucide-react';

export type ProcessFeedbackTone = 'idle' | 'processing' | 'success' | 'partial' | 'error';

export interface ProcessFeedbackCardProps {
  tone: ProcessFeedbackTone;
  title: string;
  description: string;
  persistenceHint?: string;
  nextStep?: string;
  className?: string;
}

const TONE_STYLES: Record<
  ProcessFeedbackTone,
  {
    container: string;
    iconBg: string;
    iconColor: string;
    titleColor: string;
    textColor: string;
    hintColor: string;
    nextColor: string;
    icon: React.ComponentType<{ className?: string }>;
  }
> = {
  idle: {
    container: 'border-slate-200 bg-slate-50/80',
    iconBg: 'bg-slate-100',
    iconColor: 'text-slate-500',
    titleColor: 'text-slate-800',
    textColor: 'text-slate-600',
    hintColor: 'text-slate-500',
    nextColor: 'text-slate-700',
    icon: Sparkles,
  },
  processing: {
    container: 'border-blue-200 bg-blue-50',
    iconBg: 'bg-blue-100',
    iconColor: 'text-blue-600',
    titleColor: 'text-blue-900',
    textColor: 'text-blue-700',
    hintColor: 'text-blue-600',
    nextColor: 'text-blue-800',
    icon: Loader2,
  },
  success: {
    container: 'border-emerald-200 bg-emerald-50',
    iconBg: 'bg-emerald-100',
    iconColor: 'text-emerald-600',
    titleColor: 'text-emerald-900',
    textColor: 'text-emerald-700',
    hintColor: 'text-emerald-600',
    nextColor: 'text-emerald-800',
    icon: CheckCircle2,
  },
  partial: {
    container: 'border-amber-200 bg-amber-50',
    iconBg: 'bg-amber-100',
    iconColor: 'text-amber-600',
    titleColor: 'text-amber-900',
    textColor: 'text-amber-700',
    hintColor: 'text-amber-600',
    nextColor: 'text-amber-800',
    icon: AlertTriangle,
  },
  error: {
    container: 'border-rose-200 bg-rose-50',
    iconBg: 'bg-rose-100',
    iconColor: 'text-rose-600',
    titleColor: 'text-rose-900',
    textColor: 'text-rose-700',
    hintColor: 'text-rose-600',
    nextColor: 'text-rose-800',
    icon: AlertCircle,
  },
};

const ProcessFeedbackCard: React.FC<ProcessFeedbackCardProps> = ({
  tone,
  title,
  description,
  persistenceHint,
  nextStep,
  className = '',
}) => {
  const style = TONE_STYLES[tone];
  const Icon = style.icon;

  return (
    <div className={`rounded-2xl border p-4 shadow-sm ${style.container} ${className}`}>
      <div className="flex items-start gap-3">
        <div className={`mt-0.5 flex h-10 w-10 items-center justify-center rounded-2xl ${style.iconBg}`}>
          <Icon className={`h-5 w-5 ${style.iconColor} ${tone === 'processing' ? 'animate-spin' : ''}`} />
        </div>
        <div className="min-w-0 flex-1">
          <div className={`text-sm font-semibold ${style.titleColor}`}>{title}</div>
          <div className={`mt-1 text-sm leading-6 ${style.textColor}`}>{description}</div>
          {persistenceHint ? (
            <div className={`mt-2 text-xs ${style.hintColor}`}>主流程状态：{persistenceHint}</div>
          ) : null}
          {nextStep ? (
            <div className={`mt-2 text-sm font-medium ${style.nextColor}`}>下一步建议：{nextStep}</div>
          ) : null}
        </div>
      </div>
    </div>
  );
};

export default ProcessFeedbackCard;

