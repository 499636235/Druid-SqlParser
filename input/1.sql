with late as (select agentcode,
                      agentgroup,
                      managecom,
                      agentseries,
                      trim(agentgrade) as agentgrade, -- 部分值存在空格
                      agentlastseries,
                      agentlastgrade,
                      introagency,
                      upagent,
                      othupagent,
                      introbreakflag,
                      introcommstart,
                      introcommend,
                      edumanager,
                      rearbreakflag,
                      rearcommstart,
                      rearcommend,
                      ascriptseries,
                      oldstartdate,
                      oldenddate,
                      startdate,
                      astartdate,
                      assesstype,
                      state,
                      branchcode,
                      trim(lastagentgrade1) as lastagentgrade1, -- 部分值存在空格
                      trim(agentgrade1) as agentgrade1, -- 部分值存在空格
                      employgrade,
                      blacklisflag,
                      reasontype,
                      reason,
                      uwclass,
                      uwlevel,
                      uwmodifytime,
                      uwmodifydate,
                      connmanagerstate,
                      tollflag,
                      branchtype,
                      branchtype2,
                      agentkind,
                      difficulty,
                      agentgradersn,
                      connsuccdate,
                      insideflag,
                      agentline,
                      isconnman,
                      initgrade,
                      speciflag,
                      specistartdate,
                      specienddate,
                      vipproperty,
                      connsuccdate2,
                      vipstartdate,
                      id,
                      create_date,
                      update_date,
                      create_by,
                      update_by,
                      del_flag,
                      baslawcode,
                      remarks,
                      orgstrucode,
                      distictcode,
                      gradeproperty,
                      isybnewser
                 from soochow_data.ods_cmsp_latree)

SELECT laat.agentcode as laagentcode,
late.branchtype as branchtype,
         substr(laqn.grantdate, 1, 10) as zygrantdate,
         if(laqn1.qualifno = 'aaaa', '', laqn1.qualifno) as dlrqualifno,
         case
           when tem_ll.re = 1 and laat.agentstate = '10' then
            '1'
           else
            0
         end as needapproveflag,
         (case late.branchtype
           when '1' then
            syst12.label
           when '2' THEN
            syst13.label
           when '3' then
            syst14.label
           when '4' then
            (case late.agentgrade
              when 'A1' then
               '见习收展员'
              when 'A2' THEN
               '观察期收展员'
              WHEN 'A3' then
               '正式收展员'
              when 'A4' then
               '高级收展员'
              else
               syst15.label
            end)
           else
            '其他'
         end) as agentgradename,
         'IN' OGGACTION,
         from_unixtime(unix_timestamp(), 'yyyy-MM-dd') OGGDATE,
         from_unixtime(unix_timestamp(), 'yyyy-MM-dd') PushDate,
         from_unixtime(unix_timestamp(), 'HH:mm:ss') PushTime
    FROM soochow_data.ods_cmsp_laagent laat
    left join late
      on late.agentcode = laat.agentcode
    left join soochow_data.ods_cmsp_larearrelation larn
      on laat.agentcode = larn.agentcode
     AND larn.del_flag = '0'
     AND larn.rearedgens = 1
     AND larn.rearlevel = '01'
     AND larn.enddate IS NULL
     AND larn.rearflag = '1'
     AND larn.baslawcode = late.baslawcode
    left join soochow_data.ods_cmsp_larearrelation larn1
      on laat.agentcode = larn1.agentcode
     AND larn1.del_flag = '0'
     AND larn1.rearedgens = 2
     AND larn1.rearlevel = '01'
     AND larn1.enddate IS NULL
     AND larn1.rearflag = '1'
     AND larn1.baslawcode = late.baslawcode
    left join soochow_data.ods_cmsp_larearrelation larn2
      on laat.agentcode = larn2.agentcode
     AND larn2.del_flag = '0'
     AND larn2.rearedgens = 1
     AND larn2.rearlevel = '02'
     AND larn2.enddate IS NULL
     AND larn2.rearflag = '1'
     AND larn2.baslawcode = late.baslawcode
    left join soochow_data.ods_cmsp_larearrelation larn3
      on laat.agentcode = larn3.agentcode
     AND larn3.del_flag = '0'
     AND larn3.rearedgens = 2
     AND larn3.rearlevel = '02'
     AND larn3.enddate IS NULL
     AND larn3.rearflag = '1'
     AND larn3.baslawcode = late.baslawcode
    left join soochow_data.ods_cmsp_labranchgroup labp
      on labp.agentgroup = laat.agentgroup
     and labp.del_flag = '0'
    left join soochow_data.ods_cmsp_sys_office syse
      on syse.code = laat.managecom
    -- and syse.del_flag = '0'
    left join soochow_data.ods_cmsp_sys_dict syst
      on syst.type = 'sex'
     AND syst.del_flag = '0'
     AND syst.VALUE = laat.sex
    left join soochow_data.ods_cmsp_sys_dict syst1
      on syst1.type = 'degree'
     AND syst1.del_flag = '0'
     AND syst1.VALUE = laat.degree
    left join (SELECT labt.bankaccount,
                      labt.agentcode,
                      row_number() over(partition by agentcode) rank
                 FROM soochow_data.ods_cmsp_labankaccount labt) tem
      on tem.agentcode = laat.agentcode
     and tem.rank = 1
    left join soochow_data.ods_cmsp_sys_dict syst2
      on syst2.type = 'agentstate'
     AND syst2.del_flag = '0'
     AND syst2.VALUE = laat.agentstate
    left join soochow_data.ods_cmsp_sys_dict syst3
      on syst3.type = 'branchtype'
     AND syst3.del_flag = '0'
     AND syst3.VALUE = laat.branchtype
    left join soochow_data.ods_cmsp_labranchgroup labp1
      on labp1.agentgroup = laat.branchcode
     and labp1.del_flag = '0'
    left join soochow_data.ods_cmsp_sys_dict syst4
      on syst4.type = 'idtype'
     AND syst4.del_flag = '0'
     AND syst4.VALUE = laat.idtype
    left join soochow_data.ods_cmsp_laqualification laqn
      on laqn.idx = '2'
     AND laqn.agentcode = laat.agentcode
     AND laqn.del_flag = '0'
    left join soochow_data.ods_cmsp_laqualification laqn1
      on laqn1.idx = '1'
     AND laqn1.agentcode = laat.agentcode
     AND laqn1.del_flag = '0'
    left join (SELECT 1 re,
                      LL.agentcode,
                      row_number() over(partition by agentcode) rank
                 FROM soochow_data.ods_cmsp_laqualification LL
                where LL.idx = '2'
                  AND LL.del_flag = '0') tem_ll
      on tem_ll.agentcode = laat.agentcode
     and tem_ll.rank = 1
    left join soochow_data.ods_cmsp_laauthorize laae
      on laae.branchtype = '2'
     AND laae.authorobj = laat.agentcode
     AND laae.riskcode = '2'
     AND laae.authortype = '0'
    left join soochow_data.ods_cmsp_sys_office syse1
      on syse1.code = late.managecom
    -- and syse1.del_flag = '0'
    left join soochow_data.ods_cmsp_ldcoderela ldca
      on ldca.RelaType = 'comtoareatype'
     and ldca.code1 = substr(late.managecom, 1, 6)
     and ldca.code3 = '3'
    left join soochow_data.ods_cmsp_sys_dict syst12
      on syst12.type = 'grade'
     AND syst12.del_flag = '0'
     AND syst12.VALUE = late.agentgrade
    left join soochow_data.ods_cmsp_sys_dict syst13
      on syst13.type = 'agentgrade1'
     AND syst13.del_flag = '0'
     AND syst13.VALUE = late.agentgrade
    left join soochow_data.ods_cmsp_sys_dict syst14
      on syst14.type = 'agentgrade'
     AND syst14.del_flag = '0'
     AND syst14.VALUE = late.agentgrade
    left join soochow_data.ods_cmsp_sys_dict syst15
      on syst15.type = 'sz_bonus_agent_grade'
     AND syst15.del_flag = '0'
     AND syst15.VALUE = late.agentgrade
    left join soochow_data.ods_cmsp_labranchgroup labp2
      on labp2.agentgroup = late.branchcode
     and labp2.del_flag = '0'
    left join soochow_data.ods_cmsp_sys_dict syst16
      on syst16.type = 'grade'
     AND syst16.del_flag = '0'
     AND syst16.VALUE = late.agentgrade1
    left join soochow_data.ods_cmsp_sys_dict syst5
      on syst5.type = 'agentgrade1'
     AND syst5.del_flag = '0'
     AND syst5.VALUE = late.agentgrade1
    left join soochow_data.ods_cmsp_sys_dict syst6
      on syst6.type = 'agentgrade'
     AND syst6.del_flag = '0'
     AND syst6.VALUE = late.agentgrade1
    left join soochow_data.ods_cmsp_sys_dict syst7
      on syst7.type = 'sz_bonus_agent_grade'
     AND syst7.del_flag = '0'
     AND syst7.VALUE = late.agentgrade1
    left join soochow_data.ods_cmsp_sys_dict syst8
      on syst8.type = 'grade'
     AND syst8.del_flag = '0'
     AND syst8.VALUE = late.initgrade
    left join soochow_data.ods_cmsp_sys_dict syst9
      on syst9.type = 'agentgrade1'
     AND syst9.del_flag = '0'
     AND syst9.VALUE = late.initgrade
    left join soochow_data.ods_cmsp_sys_dict syst10
      on syst10.type = 'agentgrade'
     AND syst10.del_flag = '0'
     AND syst10.VALUE = late.initgrade
    left join soochow_data.ods_cmsp_sys_dict syst11
      on syst11.type = 'sz_bonus_agent_grade'
     AND syst11.del_flag = '0'
     AND syst11.VALUE = late.initgrade
    left join soochow_data.ods_cmsp_ldcode ldce
      on ldce.codetype = 'vipproperty'
     AND ldce.del_flag = '0'
     AND ldce.CODE = late.vipproperty
    left join soochow_data.ods_cmsp_labaslawinfo labo
      on labo.baslawcode = late.baslawcode
     and labo.del_flag = '0'