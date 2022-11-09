"""
A mock implementation of the Salesforce customer record service.  This is intended as a stand-in for 
a Salesforce service for testing purposes.  
"""
import os, sys, re, smtplib, time
from collections import OrderedDict
from email.mime.text import MIMEText

class RestrictedAccessCustomerService:
    """
    A service for tracking end-user requests for restricted public data.  The request data, email 
    communications, and status are stored in flat files on disk.
    """
    def_from = "Customer Service <rpd@nist.gov>"

    def __init__(self, storedir, idpfx="sf-", dosendfrom=None):
        """
        initialize the service
        :param str    storedir:  the base directory where all request data will be stored on disk
        :param str       idpfx:  a prefix to start all minted identifiers with 
        :param bool dosendfrom:  if not None, an attempt will be made to actually send the email with 
                                 a from addresss given by this value.  If None, an email will not actually 
                                 be sent; if will just be archived.
        """
        self.dir = storedir
        self.pfx = idpfx
        self.next = self._last_index() + 1
        self.emailfrom = dosendfrom

    def _last_index(self):
        idxs = [int(d[len(idpfx):]) for d in self.ids() if re.match(self.pfx+r'\d+$')]
        if len(idxs) == 0:
            return 0
        idxs.sort()
        return idxs[-1]

    def get_request(self, id):
        """
        return the request data for a request with the given identifier
        """
        recdir = os.path.join(self.dir, id)
        if not os.path.exists(recdir):
            return None

        with open(os.path.join(recdir, "data.json")) as fd:
            return json.load(fd, object_pairs_hook=OrderedDict)

    def ids(self):
        """
        return a list of all identifiers that have been created
        """
        return [d for d in os.listdir(self.dir) if d.startswith(self.pfx)]

    def _take_next_id(self):
        out = self.next
        self.next += 1
        return out

    def mint_id(self):
        return "%s%04d" % (self.pfx, self._take_next_id())

    def create_request(self, reqdata):
        """
        create a request record with the given data, assigning an id
        @return the full request record with the given data and the assigned identifier embedded in it
                @rtype Mapping
        """
        id = self.mint_id()
        rec = OrderedDict([
            ("id", id), 
            ("request_data", reqdata)
            ("approval_status", "pending")
        ])
        recdir = os.path.join(self.dir, id)
        if not os.path.exists(recdir):
            os.mkdir(recdir)
        with open(os.path.join(recdir, "data.json"), 'w') as fd:
            json.dump(rec, fd, indent=2)

        return rec

    def send_email_for(self, id, recipients, subject, content):
        """
        send an email to the specified recipient with regard to the request specified by the given 
        identifier.  In addition to sending the email, a copy of the email will be stored with the 
        record.
        """
        recdir = os.path.join(self.dir, id)
        if not os.path.exists(recdir):
            return None

        msg = self._make_message(recipients, subject, content)
        if self.emailfrom:
            self._do_send(self.emailfrom, recipients, msg)
        return self._archive_email(id, self.emailfrom, recipients, msg)

    def _make_message(self, recipients, subject, body):
        msg = MIMEText(body)

        msg['From'] = self.emailfrom or self.deffrom
        msg['To'] = ", ".join(recipients)
        msg['Subject'] = subject
        
        return msg.as_string(False, 78)

    def _do_send(self, fromaddr, addrs, message=""):
        """
        actually send an email to a list of addresses

        :param str    from:  the email address to indicate as the origin of the message
                           
        :param list  addrs:  a list of (raw) email addresses to send the email to
        :param str message:  the formatted contents (including the header) to send.
        """
        smtp = smtplib.SMTP(self._server, self._port)
        smtp.sendmail(fromaddr, addrs, message)
        smtp.quit()

    def _archive_email(self, id, fromaddr, recipients, message):
        recdir = os.path.join(self.dir, id)
        if not os.path.exists(recdir):
            raise RuntimeError("Identifier not found: "+id)

        when = time.time()
        mailf = os.path.join(recdir, "%s-%s.txt" % (receipients[0], str(when)))
        with open(mailf, 'w') as fd:
            fd.write("To ")
            fd.write(" ".join(recipients))
            fd.write("\n")
            fd.write("From "+fromaddr)
            fd.write("\n")
            fd.write(message)

    def update_status(self, id, status):
        """
        update the status of a request.  This implementation does not care what the status value is.
        :param str id:       the identifier for the record whose status should be updated
        :param str status:   the status string to set.
        @return the full request record with the given status included, or None if the record was 
                not found.  
                @rtype Mapping
        """
        recdir = os.path.join(self.dir, id)
        if not os.path.exists(recdir):
            return None
        recf = os.path.join(recdir, "data.json")

        with open(recf) as fd:
            rec = json.load(fd, object_pairs_hook=OrderedDict)

        rec['approval_status'] = status
        with open(recf, 'w') as fd:
            json.dump(rec, fd, indent=2)

        return rec



    
        
        
        
