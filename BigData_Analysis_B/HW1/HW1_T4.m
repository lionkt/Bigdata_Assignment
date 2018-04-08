clc;
[data,txt1,~] = xlsread('./data.xlsx','data');
[category_data,txt2,~] = xlsread('./data.xlsx','category_info');
category_label = category_data(:,1);
category_num = category_data(:,3);

%% T4
test_pool = [3;4;5];
disp('========T4的KS检验结果');
for th_ = 1:length(test_pool)
    [em_f,em_x] = ecdf(data(:,test_pool(th_)));
    alpha = 0.05;
    [mu, sigma] = normfit(data(:,test_pool(th_)));
    ref_cdf = normcdf(em_x,mu,sigma);
    [H_th,p_th] = kstest(data(:,test_pool(th_)),'CDF',[em_x,ref_cdf],'Alpha',alpha);   %ks检验
    disp(['数据',num2str(test_pool(th_)),'的H:',num2str(H_th),', p-value:',num2str(p_th)]);
    
    figure;
    subplot(121);
    F = plot(em_x,em_f);
    set(F,'LineWidth',2);
%     hold on;
%     G = plot(em_x, ref_cdf, 'r-');
%     set(G,'LineWidth',2);
%     legend([F G],'Empirical CDF','Standard Normal CDF(fit)','Location','SE');
    xlabel('x-axes');
    grid on;title(['数据',num2str(test_pool(th_)),'的CDF']);
    
    subplot(122);
    [pdf_y,pdf_x] = ksdensity(data(:,test_pool(th_)));
    F = plot(pdf_x,pdf_y);
    set(F,'LineWidth',2);
%     hold on;
%     G = plot(em_x, normpdf(em_x,mu,sigma), 'r-');
%     set(G,'LineWidth',2);
%     legend([F G],'Empirical PDF','Standard Normal PDF(fit)','Location','SE');
    xlabel('x-axes');title(['数据',num2str(test_pool(th_)),'的PDF']);
    grid on;
    
    befor_log_data = data(:,test_pool(th_));
    data_by_cate = zeros(length(category_label),length(befor_log_data));
    data_by_cate_length = zeros(length(category_label),1);
    max_length = 0;
    for i=1:length(category_label)
        ixx = find(data(:,2)==category_label(i));
        data_by_cate(i,1:length(ixx)) = befor_log_data(ixx);
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
    for i=1:length(category_label)
        test_data = data_by_cate(i,1:data_by_cate_length(i));
        [f_i,x_i] = ecdf(test_data);
        [mu_i, sigma_i] = normfit(test_data);
        ref_cdf_i = normcdf(x_i,mu_i,sigma_i);
        [H_i,p_i] = kstest(test_data,'CDF',[x_i,ref_cdf_i],'Alpha',alpha);   %ks检验
        H_list(i) = H_i; p_list(i) = p_i;
        mean_list(i) = mean(test_data);
        var_list(i) = var(test_data);
    end
    if max(sqrt(var_list)) > 2*min(sqrt(var_list))
        disp(['数据',num2str(test_pool(th_)),'方差不满足齐性']);
    else
        disp(['数据',num2str(test_pool(th_)),'方差满足齐性']);
    end
end

disp('========T4的log变换KS检验结果');
for th_ = 1:length(test_pool)
    log_data = log(data(:,test_pool(th_)));
    [em_f,em_x] = ecdf(log_data);
    alpha = 0.05;
    [mu, sigma] = normfit(log_data);
    ref_cdf = normcdf(em_x,mu,sigma);
    [H_th,p_th] = kstest(log_data,'CDF',[em_x,ref_cdf],'Alpha',alpha);   %ks检验
    disp(['数据',num2str(test_pool(th_)),'的H:',num2str(H_th),', p-value:',num2str(p_th)]);
    
    figure;
    subplot(121);
    F = plot(em_x,em_f);
    set(F,'LineWidth',2);
    hold on;
    G = plot(em_x, ref_cdf, 'r-');
    set(G,'LineWidth',2);
    legend([F G],'Empirical CDF','Standard Normal CDF(fit)','Location','SE');
    grid on;title(['数据',num2str(test_pool(th_)),'的CDF']);
    
    subplot(122);
    [pdf_y,pdf_x] = ksdensity(log_data);
    F = plot(pdf_x,pdf_y);
    set(F,'LineWidth',2);
    hold on;
    G = plot(em_x, normpdf(em_x,mu,sigma), 'r-');
    set(G,'LineWidth',2);
    title(['数据',num2str(test_pool(th_)),'的PDF']);
    legend([F G],'Empirical PDF','Fitting Normal PDF','Location','SE');
    grid on;
    
    data_by_cate = zeros(length(category_label),length(log_data));
    data_by_cate_length = zeros(length(category_label),1);
    max_length = 0;
    for i=1:length(category_label)
        ixx = find(data(:,2)==category_label(i));
        data_by_cate(i,1:length(ixx)) = log_data(ixx);
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
    for i=1:length(category_label)
        test_data = data_by_cate(i,1:data_by_cate_length(i));
        [f_i,x_i] = ecdf(test_data);
        [mu_i, sigma_i] = normfit(test_data);
        ref_cdf_i = normcdf(x_i,mu_i,sigma_i);
        [H_i,p_i] = kstest(test_data,'CDF',[x_i,ref_cdf_i],'Alpha',alpha);   %ks检验
        H_list(i) = H_i; p_list(i) = p_i;
        mean_list(i) = mean(test_data);
        var_list(i) = var(test_data);
    end
    if max(sqrt(var_list)) > 2*min(sqrt(var_list))
        disp(['数据',num2str(test_pool(th_)),'方差不满足齐性']);
    else
        disp(['数据',num2str(test_pool(th_)),'方差满足齐性']);
    end
end