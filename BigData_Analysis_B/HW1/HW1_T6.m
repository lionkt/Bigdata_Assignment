clc;
[data,txt1,~] = xlsread('./data.xlsx','data');
[category_data,txt2,~] = xlsread('./data.xlsx','category_info');
category_label = category_data(:,1);
category_num = category_data(:,3);

cate_pool = [1;4];          % 需要分类的群组
feature_pool = [3:1:14];    % 特征数据
Start_Index_Fix = 2;
feature_start = 4;
feature_end = 9;
feature_start = feature_start-Start_Index_Fix;
feature_end = feature_end-Start_Index_Fix;

ixx1 = find(data(:,2)==cate_pool(1));
ixx2 = find(data(:,2)==cate_pool(2));
max_c_num = 0;
for i=1:length(feature_pool)
    temp_num = nchoosek(length(feature_pool),i);
    if max_c_num < temp_num
        max_c_num = temp_num;
    end
end
accuracy_list = zeros(length(feature_pool),max_c_num);  % 正确率的list


for num_ = 1:length(feature_pool)
    disp(['--- 开始处理',num2str(num_),'个feature的组合']);
    temp_feature_list = nchoosek(feature_pool,num_);    % num_个feature可能的组合列表
    for th_ = 1:size(temp_feature_list,1)
        test_data1 = data(ixx1, temp_feature_list(th_,:));
        test_data2 = data(ixx2, temp_feature_list(th_,:));
%         test_data1 = log(test_data1 + 1);
%         test_data2 = log(test_data2 + 1);
        %%% 归一化处理
%         test_data1 = zscore(test_data1);
%         test_data2 = zscore(test_data2);
        X = [test_data1;test_data2];
        Y = [data(ixx1,2);data(ixx2,2)];
        binary_Y = Y;
        binary_Y(Y==min(Y)) = 0;
        binary_Y(Y==max(Y)) = 1;
        theta = glmfit(X,binary_Y,'binomial', 'link', 'logit');    % 2元logistical回归
        %%% 检查正确率
        reconstruct_Y = [ones(size(X,1),1), X]*theta;
        res_index1 = find(reconstruct_Y<mean(cate_pool));
        res_index2 = find(reconstruct_Y>=mean(cate_pool));
        reconstruct_Y(res_index1) = min(cate_pool);
        reconstruct_Y(res_index2) = max(cate_pool);
        error = reconstruct_Y-Y;
        error_rate = length(find(error~=0))/length(error);
        accuracy_list(num_,th_) = 1-error_rate;
    end
end

%% 绘制正确率box plot
accuracy_list = accuracy_list*100;
accuracy_list(accuracy_list==0) = nan;
boxplot(accuracy_list');
title('不同feature数目下各种特征组合的正确率分布');
xlabel('feature数目');ylabel('正确率 %');
grid on;

%% 输出各个数目下的最优的组合
record_list = ones(length(feature_pool),1);
max_accuracy_list = ones(length(feature_pool),1);
for th_ = 1:length(feature_pool)
    [max_accuracy_list(th_), record_list(th_)] = max(accuracy_list(th_,:));
    temp_c = nchoosek(feature_pool,th_);
    disp([num2str(th_),'个特征时的最优组合为:[',num2str(temp_c(record_list(th_),:)),'], 正确率为:',num2str(max_accuracy_list(th_)),'%']);
    
end


