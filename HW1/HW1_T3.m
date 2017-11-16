clc;
[data,txt1,~] = xlsread('./data.xlsx','data');
[category_data,txt2,~] = xlsread('./data.xlsx','category_info');
category_label = category_data(:,1);
category_num = category_data(:,3);

%% T3
%%%% a小问
% hist(data(:,7),1000);
[em_f,em_x] = ecdf(data(:,7));
alpha = 0.05;
[mu, sigma] = normfit(data(:,7));
ref_cdf = normcdf(em_x,mu,sigma);
[H1,p1] = kstest(data(:,7),'CDF',[em_x,ref_cdf],'Alpha',alpha);   %ks检验
[H2,p2] = jbtest(data(:,7),alpha);   %jb检验

figure;
F = plot(em_x,em_f);
set(F,'LineWidth',2);
hold on;
title('Empirical CDF');
grid on;

figure;

[pdf_y,pdf_x] = ksdensity(data(:,7));
histogram(data(:,7),'Normalization','pdf');
hold on;
F = plot(pdf_x,pdf_y);
set(F,'LineWidth',2.5);
xlabel('avg age');title('Empirical PDF');
legend('histogram','fitting curve');
grid on;


%%%% b小问
data_by_cate = zeros(length(category_label),length(data(:,7)));
data_by_cate_length = zeros(length(category_label),1);
max_length = 0;
for i=1:length(category_label)
    ixx = find(data(:,2)==category_label(i));
    data_by_cate(i,1:length(ixx)) = data(ixx,7);
    data_by_cate_length(i) = length(ixx);
    if max_length<data_by_cate_length(i)
        max_length = data_by_cate_length(i);
    end
end
data_by_cate(:,max_length+1:end) = [];  %删掉多余的
H_list = zeros(length(category_label),1);
p_list = zeros(length(category_label),1);
mean_list = zeros(length(category_label),1);
var_list = zeros(length(category_label),1);
figure;
for i=1:length(category_label)
    test_data = data_by_cate(i,1:data_by_cate_length(i));
    [f_i,x_i] = ecdf(test_data);
    [mu_i, sigma_i] = normfit(test_data);
    ref_cdf_i = normcdf(x_i,mu_i,sigma_i);
    [H_i,p_i] = kstest(test_data,'CDF',[x_i,ref_cdf_i],'Alpha',alpha);   %ks检验
    H_list(i) = H_i; p_list(i) = p_i;
    mean_list(i) = mean(test_data);
    var_list(i) = var(test_data);
    [pdf_y_i,pdf_x_i] = ksdensity(zscore(test_data));
    plot(pdf_x_i,pdf_y_i,'linewidth',2);
    hold on;
end
xlabel('avg age(normalized)');title('标准化后各群组的的PDF');
legend('g1','g2','g3','g4','g5');grid on;
if max(sqrt(var_list)) > 2*min(sqrt(var_list))
    disp('方差不满足齐性');
else
    disp('方差满足齐性');
end

    


%%%% c小问
SSb=0; SSw=0;
grand_mean = mean(data(:,7));
for i=1:length(category_label)
    test_data = data_by_cate(i,1:data_by_cate_length(i));
    SSb = SSb + data_by_cate_length(i)*(mean_list(i)-grand_mean)^2;
    SSw = SSw + (test_data-mean_list(i))*(test_data-mean_list(i))';
end
dfb = length(category_label)-1;
dfw = sum(data_by_cate_length)-length(category_label);
MSb = SSb/dfb;
MSw = SSw/dfw;
F = MSb/MSw;
p = 1-fcdf(F,dfb,dfw);
% p_matlab_std = anova1(data(:,7),data(:,2)); %matlab 自带的one-way anova
%%%% 画F分布
% xxx = [0.001:0.001:200];
% figure;
% plot(xxx,fpdf(xxx,dfb,dfw),'linewidth',1.7);
% hold on;
% xxx_std = [finv(1-alpha,dfb,dfw):0.001:max(xxx)];
% plot(xxx_std,fpdf(xxx_std,dfb,dfw),'linewidth',3);
% % fill([xxx_std(1)-0.0001,xxx_std],[0,fpdf(xxx_std,dfb,dfw)],[0.8706 0.9216 0.9804]);
% grid on;
% xlabel('log(F-value)');
% set(gca,'xscale','log');




