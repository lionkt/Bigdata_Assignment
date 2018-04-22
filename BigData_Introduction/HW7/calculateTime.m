clc;
clear;
mem_start = ['02:58:43';'02:59:50';'03:00:44'];
mem_end = ['05:36:12';'03:25:47';'03:20:27'];

e1core2_start = '03:13:12';
e1core2_end = '03:28:59';
e2core1_start = '03:30:32';
e2core1_end = '03:44:21';
e2core2_start = '03:13:56';
e2core2_end = '03:25:14';
e4core4_start = '03:15:28';
e4core4_end = '03:20:36';
e4core1_start = '03:16:58';
e4core1_end = '03:26:55';

mem_time = zeros(1,3);
for i=1:size(mem_start,1)
    mem_time(i) = 24*(datenum(mem_end(i,:),'HH:MM:SS')-datenum(mem_start(i,:),'HH:MM:SS'));
end
e1core1_time = mem_time(2);
e1core2_time = 24*(datenum(e1core2_end,'HH:MM:SS')-datenum(e1core2_start,'HH:MM:SS'));
e2core1_time = 24*(datenum(e2core1_end,'HH:MM:SS')-datenum(e2core1_start,'HH:MM:SS'));
e2core2_time = 24*(datenum(e2core2_end,'HH:MM:SS')-datenum(e2core2_start,'HH:MM:SS'));
e4core4_time = 24*(datenum(e4core4_end,'HH:MM:SS')-datenum(e4core4_start,'HH:MM:SS'));
e4core1_time = 24*(datenum(e4core1_end,'HH:MM:SS')-datenum(e4core1_start,'HH:MM:SS'));

%% 
figure;
plot([2,4,8],mem_time,'o--','linewidth',1.5);
title('runtime on diffirent memory (1 executor 1 core)');
ylabel('time (h)');xlabel('memory(GB)');
grid on;


figure;
plot([1,2],[e1core1_time,e1core2_time],'o:','linewidth',1.5); hold on;
plot([1,2],[e2core1_time, e2core2_time],'*:','linewidth',1.5); hold on;
plot([1,4],[e4core1_time,e4core4_time],'d:','linewidth',1.5); hold on;
legend('1 executor','2 executors','4 executors');
title('runtime on diffirent executor and cores (4GB)');
ylabel('time (h)');xlabel('core number (on each executor)');
grid on;






