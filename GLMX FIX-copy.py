try:
    import time
    import os
    import sys
    import ASL
    import argparse
    import pandas as pd
    from datetime import datetime, date, timedelta
    import quickfix as fix
    import quickfix44 as fix44
    import redis, json

    channel = 'GLMX_INQUIRY'
    r = redis.Redis(host='cacheprod',decode_responses=True)
    ps = r.pubsub(ignore_subscribe_messages=True)
    ps.psubscribe(channel)


    class Application(fix.Application):

            quoteID = 0
            msgID = 0

            def onCreate(self, sessionID):
                print("onCreate : Session (%s)" % sessionID.toString())
                return
            def onLogon(self, sessionID): 
                self.sessionID = sessionID
                print("Successful Logon to session '%s'." % sessionID.toString())
                print()
                return
            def onLogout(self, sessionID): 
                print("Successful Logout from session '%s'." % sessionID.toString())
                return
            def toAdmin(self, message, sessionID): 
                self.sessionID = sessionID
                print(" toAdmin: %s " % str(message))
                if(message.getHeader().getField(fix.MsgType().getField()) == "A"):
                    print(" login Message " + str(sessionID))
                #     message.setField(fix.Username("ASL_PRE"))
                    message.setField(fix.Password("fu7taXJRsbMVPTyU"))
                #     fix.Session.sendToTarget(message, sessionID)
                return
            def fromAdmin(self, message, sessionID): 
                print("fromAdmin: %s" % message.toString())
                return
            def toApp(self, message, sessionID): 
                print("ToApp: %s" % message.toString())
                return
            def fromApp(self, message, sessionID):
                
                if(message.getHeader().getField(fix.MsgType().getField()) == "R") or (message.getHeader().getField(fix.MsgType().getField()) == "AJ"):
                    print(" FromApp: %s " % message.toString())
                    quote = self.quote(message)
                    if quote != 'disregard':
                        try:
                            fix.Session.sendToTarget( quote, sessionID )
                            print()
                        except fix.SessionNotFound as e:
                            return
                return 



            def quote(self, message):
                # Parse message
                if message.getHeader().getField(fix.MsgType().getField()) == "AJ":
                    subgroup = fix44.QuoteResponse.NoUnderlyings()
                    subsubgroup = fix44.QuoteResponse.NoUnderlyings.NoPartyIDs()

                    qrid =  message.getField(fix.QuoteReqID().getField())
                    non = message.getField(fix. NoUnderlyings().getField())
                    stts = message.getField(fix.QuoteRespID().getField())
                    type = message.getField(fix.QuoteRespType().getField())
                    if type != '2':
                        print('QuoteType not counter!')
                        return 'disregard'
                    else:
                        pass
                    message.getGroup(1, subgroup)
                    sd = subgroup.getField(fix.StartDate().getField())
                    ed = subgroup.getField(fix.EndDate().getField())
                    numid = subgroup.getField(fix.NoPartyIDs().getField())
                    
                    for i in range(1, int(numid)+1):
                        subgroup.getGroup(i, subsubgroup)
                        if subsubgroup.getField(fix.PartyRole().getField()) == '3':
                            cust = subsubgroup.getField(fix.PartyID().getField())
                            break
                    print(cust)
                

                else:
                    group = fix44.QuoteRequest.NoRelatedSym()
                    subgroup = fix44.QuoteRequest.NoRelatedSym.NoUnderlyings()
                    subsubgroup = fix44.QuoteRequest.NoRelatedSym.NoUnderlyings.NoPartyIDs()
                    message.getGroup(1, group)
                    qrid =  message.getField(fix.QuoteReqID().getField())
                    non = group.getField(fix. NoUnderlyings().getField())
                    
                    group.getGroup(1, subgroup)
                    sd = subgroup.getField(fix.StartDate().getField())
                    ed = subgroup.getField(fix.EndDate().getField())
                    numid = subgroup.getField(fix.NoPartyIDs().getField())
                    
                    for i in range(1, int(numid)+1):
                        subgroup.getGroup(i, subsubgroup)
                        if subsubgroup.getField(fix.PartyRole().getField()) == '3':
                            cust = subsubgroup.getField(fix.PartyID().getField())
                            break
                    print(cust)
                
                # cash VS reg
                holiday = pd.read_excel('//aslfile01/ASLCAP/tblHolidays.xlsm')
                holiday = holiday['HolidayDate'].apply(lambda x: str(x)[0:10]).to_list()
                
                tod = date.today()
                tom = tod + timedelta(days = 1)
                tom = self.holy(tod, tom, holiday)
                tomtom = tom + timedelta(days = 1)
                tomtom = self.holy(tom, tomtom, holiday)
                
                
                
                if ((sd == tod.strftime("%Y%m%d")) and (ed == tom.strftime("%Y%m%d"))) or ((sd == tom.strftime("%Y%m%d")) and (ed == tomtom.strftime("%Y%m%d"))):
                    list = []
                    for i in range(1,int(non)+1):
                        if message.getHeader().getField(fix.MsgType().getField()) == "AJ":
                            message.getGroup(i, subgroup)
                        else:
                            group.getGroup(i, subgroup)
                        hai = subgroup.getField(fix.UnderlyingSecurityDesc().getField())
                        # if hai != 'US T All' and hai != 'US T Under 2':
                        #     hc = datetime.strptime(hai[-8:], '%m/%d/%y').date()
                        #     gid = subgroup.getField(fix.StringField(5022).getField())
                        #     sou = subgroup.getField(fix.UnderlyingSecurityIDSource().getField())
                        #     if sou == '1':
                        #         cus = subgroup.getField(fix.UnderlyingSecurityID().getField())
                        #     elif sou == '4':
                        #         cus = subgroup.getField(fix.UnderlyingSecurityID().getField())[2:11]
                        #     else:
                        #         print('NEW305:', sou)
                        #         cus = subgroup.getField(fix.UnderlyingSecurityID().getField())
                        #     typ = subgroup.getField(fix.Side().getField())
                        #     list.extend((hc, gid, cus, typ))
                        # else:
                        #     print('Not specific treasury!')
                        #     return 'disregard'
                        try:
                            if message.getHeader().getField(fix.MsgType().getField()) == "AJ":
                                try:
                                    subgroup.getField(fix.Price().getField())
                                except:
                                    pass
                                else:
                                    raise ValueError
                            hc = datetime.strptime(hai[-8:], '%m/%d/%y').date()
                            gid = subgroup.getField(fix.StringField(5022).getField())
                            cusip = subgroup.getField(fix.UnderlyingSecurityID().getField())
                            quant = subgroup.getField(fix.UnderlyingQty().getField())
                            sdd = subgroup.getField(fix.StartDate().getField())
                            edd = subgroup.getField(fix.EndDate().getField())
                            sou = subgroup.getField(fix.UnderlyingSecurityIDSource().getField())
                            if sou == '1':
                                cus = subgroup.getField(fix.UnderlyingSecurityID().getField())
                            elif sou == '4':
                                cus = subgroup.getField(fix.UnderlyingSecurityID().getField())[2:11]
                            else:
                                print('NEW305:', sou)
                                cus = subgroup.getField(fix.UnderlyingSecurityID().getField())
                            typ = subgroup.getField(fix.Side().getField())
                            list.extend((hc, gid, cus, typ, cusip, quant, sdd, edd))
                        except ValueError:
                            print('Not specific treasury!')
                            return 'disregard'
                    
                    if cus[0] == 3:
                        print('Not treasury!')
                        return 'disregard'
                    else:
                        if (sd == tod.strftime("%Y%m%d")) and (ed == tom.strftime("%Y%m%d")):
                            print('cash')
                            if message.getHeader().getField(fix.MsgType().getField()) == "AJ":
                                quote = self.cor(list, qrid, cust, non, tod, '', stts)
                            else:
                                quote = self.cor(list, qrid, cust, non, tod, '', '')
                            return quote
                        elif (sd == tom.strftime("%Y%m%d")) and (ed == tomtom.strftime("%Y%m%d")):
                            print('reg')
                            if message.getHeader().getField(fix.MsgType().getField()) == "AJ":
                                quote = self.cor(list, qrid, cust, non, tod, '_REG', stts)
                            else:
                                quote = self.cor(list, qrid, cust, non, tod, '_REG', '')
                            return quote
                else:
                    print('Not cash/reg o/n!')
                    return 'disregard'
            

            def cor(self, list, qrid, cust, non, tod, COR, stts):
                # Quote
                beginString = fix.BeginString()
                quote = fix.Message()
                quote.getHeader().setField( beginString )
                quote.getHeader().setField( fix.MsgType(fix.MsgType_Quote) )

            
                quote.setField(fix.StringField(60,(datetime.utcnow ().strftime ("%Y%m%d-%H:%M:%S.%f"))[:-3]))
                quote.setField( fix.QuoteReqID(qrid) )
                quote.setField( fix.QuoteID(self.genQuoteID()) )
                quote.setField( fix.QuoteMsgID(self.genMsgID()) )
                if stts != '':
                    quote.setField( fix.QuoteRespID (stts) )
                # counter = 3, stage = 100
                quote.setField( fix.QuoteType(100) )
                quote.setField( fix.Symbol("[N/A]") )
                
                group = fix44.Quote.NoUnderlyings()
                subgroup = fix44.Quote.NoUnderlyings.NoPartyIDs()
                subgroup.setField( fix.PartyID('tinghe.lou@aslcap.com') )
                subgroup.setField( fix.PartyIDSource("D") )
                subgroup.setField( fix.PartyRole(11) )
                group.addGroup(subgroup)

                all = []

                # spreads
                rvotr = rvoftr = rps = oftrgc = 1
                spread = pd.read_excel(r'\\aslfile01\aslcap\Repo Desk\Customers and Marketing\GLMX Customer Spreads & Haircuts.xlsx')
                for i in range(len(spread['Counterparty'])):
                    if cust == spread['Counterparty'][i]:
                        rvotr = spread['Bid OTR'][i]
                        rvoftr = spread['Bid OFTR'][i]
                        rps = spread['Offer'][i]
                        oftrgc = spread['OFTR GC spread'][i]
                        break
                if rps == 1:
                    ASL.send_email('***ACTION REQUIRED*** Please Update GLMX Customer Spreads Sheet', cust, 'trading@aslcap.com')
                
                for i in range(0,int(non)):
                    group.setField( fix.StringField(5022, list[1+8*i]) )
                    # print(list[1+3*i])
                    if list[3+8*i] == '1':
                        gcgc = r.get("GLMX:371488AP2" + COR)
                        if gcgc is not None:
                            # RV > LAST > RP
                            if json.loads(gcgc)['RV_TIME'] is not '':
                                gc = float(json.loads(gcgc)['RV_PRICE'])
                                if gc == 0 and json.loads(gcgc)['LAST_TIME'] is not '':
                                    gc = float(json.loads(gcgc)['LAST_PRICE'])
                                    if gc == 0 and json.loads(gcgc)['RP_TIME'] is not '':
                                        gc = float(json.loads(gcgc)['RP_PRICE'])

                            # if list[2+4*i] == "91282CHH7":
                            #     print(r.get("GLMX:" + list[2+4*i]))
                            
                            if r.get("GLMX:" + list[2+8*i] + COR) is not None:
                                security = json.loads(r.get("GLMX:" + list[2+8*i] + COR))
                                if security['QTYPE'] == 'SP':
                                    rvs = rvotr
                                    sprd = 0.03
                                else:
                                    rvs = rvoftr
                                    sprd = oftrgc

                                if security['RV_TIME'] is not '':
                                    rev = float(security['RV_PRICE'])
                                    if abs(rev-gc) <= sprd:
                                        p = gc + rvs
                                    else:
                                        p = min(rev, gc) + rvs
                                elif security['LAST_TIME'] is not '':
                                    rev = float(security['LAST_PRICE'])
                                    if abs(rev-gc) <= sprd:
                                        p = gc + rvs
                                    else:
                                        p = min(rev, gc) + rvs
                                else:
                                    p = gc + rvs
                            else:
                                p = gc + rvoftr
                            
                            group.setField( fix.Price(p) )
                            # put in haircuts
                            sch = (list[0+8*i] - tod).days/365
                            if list[3+8*i] == '1':
                                if sch > 10:
                                    haic = 1
                                elif sch > 2:
                                    haic = 0.5
                                else:
                                    haic = 0
                            else:
                                haic = 0
                            for j in range(len(spread['Counterparty'])):
                                if list[3+8*i] == '1' and cust == spread['Counterparty'][j]:
                                    if sch > spread[1][j]:
                                        haic = 1
                                    elif sch > spread[0.5][j]:
                                        haic = 0.5
                                    else:
                                        haic = 0
                                    break
                                else:
                                    haic = haic
                            group.setField( fix.MarginRatio(haic) )
                            # group.setField( fix.StringField(5002, str(0)) )
                            quote.addGroup(group)

                            try:
                                qtype = security['QTYPE']
                            except Exception as err:
                                qtype = 'GC'
                            keys = ['5022', '44', '898', '54', '309', '879', '916', '917', 'qtype']
                            values = [list[1+8*i], p, haic, list[3+8*i], list[4+8*i], list[5+8*i], list[6+8*i], list[7+8*i], qtype]
                            dct = dict(zip(keys, values))
                            all.append(dct)
                        
                        else:
                            print('No GC in the market!')

                    elif (list[3+8*i] == '2') and (r.get("GLMX:" + list[2+8*i] + COR) is not None):
                        security = json.loads(r.get("GLMX:" + list[2+8*i] + COR))
                        if security['RP_TIME'] is not '':
                            rep = float(security['RP_PRICE'])
                            p = rep - rps
                        elif security['LAST_TIME'] is not '':
                            last = float(security['LAST_PRICE'])
                            p = last - 0.2
                        else:
                            rev = float(security['RV_PRICE'])
                            p = rev - 0.2
                    
                        group.setField( fix.Price(p) )
                        # put in haircuts
                        sch = (list[0+8*i] - tod).days/365
                        if list[3+8*i] == '1':
                            if sch > 10:
                                haic = 1
                            elif sch > 2:
                                haic = 0.5
                            else:
                                haic = 0
                        else:
                            haic = 0
                        for j in range(len(spread['Counterparty'])):
                            if list[3+8*i] == '1' and cust == spread['Counterparty'][j]:
                                if sch > spread[1][j]:
                                    haic = 1
                                elif sch > spread[0.5][j]:
                                    haic = 0.5
                                else:
                                    haic = 0
                                break
                            else:
                                haic = haic
                        group.setField( fix.MarginRatio(haic) )
                        # group.setField( fix.StringField(5002, str(0)) )
                        quote.addGroup(group)

                        try:
                            qtype = security['QTYPE']
                        except Exception as err:
                            qtype = 'GC'
                        keys = ['5022', '44', '898', '54', '309', '879', '916', '917', 'qtype']
                        values = [list[1+8*i], p, haic, list[3+8*i], list[4+8*i], list[5+8*i], list[6+8*i], list[7+8*i], qtype]
                        dct = dict(zip(keys, values))
                        all.append(dct)
                try:
                    p
                except NameError:
                    p = None
                
                if p is not None:
                    dic = {}
                    dic['131'] = qrid
                    dic['711'] = non
                    dic['448'] = cust
                    dic['pack'] = all
                    print(dic)
                    r.publish(channel, json.dumps(dic))
                    r.set(f"{channel}:{dic['131']}", json.dumps(dic), ex=self.get_expiry())
                    return quote
                else:
                    print('No rates in the market!')
                    return 'disregard'
            
            def get_expiry(self):
                now = datetime.now()
                midnight = datetime.combine(now + timedelta(days=1), datetime.min.time())
                return (midnight - now).seconds
            
            def requote(self, data):
                beginString = fix.BeginString()

                quote = fix.Message()
                quote.getHeader().setField( beginString )
                quote.getHeader().setField( fix.MsgType(fix.MsgType_Quote) )

            
                quote.setField(fix.StringField(60,(datetime.utcnow ().strftime ("%Y%m%d-%H:%M:%S.%f"))[:-3]))
                quote.setField( fix.QuoteReqID(data['131']) )
                quote.setField( fix.QuoteID(self.genQuoteID()) )
                quote.setField( fix.QuoteMsgID(self.genMsgID()) )
                # counter = 3, stage = 100
                quote.setField( fix.QuoteType(100) )
                quote.setField( fix.Symbol("[N/A]") )
                
                group = fix44.Quote.NoUnderlyings()
                subgroup = fix44.Quote.NoUnderlyings.NoPartyIDs()
                subgroup.setField( fix.PartyID('tinghe.lou@aslcap.com') )
                subgroup.setField( fix.PartyIDSource("D") )
                subgroup.setField( fix.PartyRole(11) )
                group.addGroup(subgroup)

                for i in range(0,len(data['pack'])):
                    group.setField( fix.StringField(5022, data['pack'][i]['5022']) )
                    # print(list[1+3*i])

                    p = data['pack'][i]['44']
                    group.setField( fix.Price(p) )
                    haic = data['pack'][i]['898']
                    group.setField( fix.MarginRatio(haic) )
                    # group.setField( fix.StringField(5002, str(0)) )
                    quote.addGroup(group)
                fix.Session.sendToTarget(quote, self.sessionID)

            
                    
            def holy(self, today, tomorrow, holiday):
                if today.weekday() == 4 and (today + timedelta(days = 3)).strftime("%Y-%m-%d") not in holiday:
                    tomorrow = today + timedelta(days = 3)
                elif today.weekday() == 4 and (today + timedelta(days = 3)).strftime("%Y-%m-%d") in holiday:
                    tomorrow = today + timedelta(days = 4)
                elif (today + timedelta(days = 1)).strftime("%Y-%m-%d") in holiday and (today + timedelta(days = 2)).strftime("%Y-%m-%d") not in holiday and (today + timedelta(days = 2)).weekday() != 5:
                    tomorrow = today + timedelta(days = 2)
                elif (today + timedelta(days = 1)).strftime("%Y-%m-%d") in holiday and (today + timedelta(days = 2)).weekday() == 5:
                    tomorrow = today + timedelta(days = 4)
                return tomorrow
            
            def genQuoteID(self):
                self.quoteID += 1
                return str(self.quoteID)
            def genMsgID(self):
                self.msgID += 1
                return str(self.msgID)
            

    def main(config_file):
        try:
                settings = fix.SessionSettings(config_file)
                application = Application()
                storeFactory = fix.FileStoreFactory(settings)
                logFactory = fix.FileLogFactory(settings)
                initiator = fix.SocketInitiator(application, storeFactory, settings, logFactory)
                

                initiator.start()
                while 1:
                    if datetime.now().strftime("%H:%M:%S") > '17:00':
                        initiator.stop()
                        print('EXIT')
                        sys.exit()
                    message = ps.get_message()  # will either be NULL or a string 
                    if message:
                        data = json.loads(message["data"])
                        try:
                            # application.requote(data)
                            if data['status'] == 'Sent':
                                    application.requote(data)
                        except KeyError:
                            print('Original/Pass/Kill messages!')
                    time.sleep(0.1)

        except fix.ConfigError as e:
                print (e)



    if __name__=='__main__':
        parser = argparse.ArgumentParser(description='FIX Client')
        parser.add_argument('-c', '--configfile', default="configfile.cfg",help='file to read the config from')
        args, unknown = parser.parse_known_args()
        main(args.configfile)

except Exception as err:
    ASL.send_email('***ACTION REQUIRED*** GLMX PROD FIX Connection is Down', err, ['tech@aslcap.com', 'joe.pizzarelli@aslcap.com'])