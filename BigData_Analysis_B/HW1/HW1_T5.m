clc;
[data,txt1,~] = xlsread('./data.xlsx','data');
[category_data,txt2,~] = xlsread('./data.xlsx','category_info');
category_label = category_data(:,1);
category_num = category_data(:,3);

test_pool = [3;4;5];
alpha = 0.05;
for th_ = 1:length(test_pool)
    data_by_cate = zeros(length(category_label),length(data(:,test_pool(th_))));
    data_by_cate_length = zeros(length(category_label),1);
    max_length = 0;
    for i=1:length(category_label)
        ixx = find(data(:,2)==category_label(i));
        data_by_cate(i,1:length(ixx)) = data(ixx,test_pool(th_));
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
        disp('方差不满足齐性');
    else
        disp('方差满足齐性');
    end
    
    SSb=0; SSw=0;
    grand_mean = mean(data(:,test_pool(th_)));
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
    
    %%% matlab 自带的one-way anova
%     p_matlab_std = anova1(log(data(:,test_pool(th_))),data(:,2)); 
end