from flask import Flask, request, jsonify, send_from_directory, send_file
from flask_cors import CORS
import hashlib
import jwt
import datetime
import os
import openpyxl
from openpyxl.styles import Font, PatternFill
from io import BytesIO
from functools import wraps
import psycopg2
import psycopg2.extras
import urllib.parse
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
import zipfile
import csv

app = Flask(__name__, static_folder='static')
app.config['SECRET_KEY'] = 'palet-takip-gizli-anahtar-2026'
CORS(app)

PALET_TIPLERI = [("P001", "Euro Palet"), ("P002", "Sanayi Paleti"), ("P003", "Plastik Palet")]
SAHIP_TIP_DEPO = "DEPO"
SAHIP_TIP_DAGITICI = "DAGITICI"
SAHIP_TIP_MUSTERI = "MUSTERI"

HAREKET_DEPO_DAGITICI = "DEPO_DAGITICI"
HAREKET_DAGITICI_MUSTERI = "DAGITICI_MUSTERI"
HAREKET_MUSTERI_DAGITICI = "MUSTERI_DAGITICI"
HAREKET_DAGITICI_DEPO = "DAGITICI_DEPO"
HAREKET_DEPO_STOK = "DEPO_STOK_HAREKET"


def hash_sifre(sifre):
    return hashlib.sha256(sifre.encode()).hexdigest()


def get_db_connection():
    database_url = os.environ.get('DATABASE_URL')
    if database_url:
        urllib.parse.uses_netloc.append('postgres')
        url = urllib.parse.urlparse(database_url)
        return psycopg2.connect(
            database=url.path[1:], user=url.username,
            password=url.password, host=url.hostname, port=url.port
        )
    else:
        import sqlite3
        conn = sqlite3.connect('palet_takip.db')
        conn.row_factory = sqlite3.Row
        return conn


def veritabani_olustur():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS kullanicilar (id SERIAL PRIMARY KEY, kullanici_adi TEXT UNIQUE NOT NULL, sifre TEXT NOT NULL, tip TEXT NOT NULL, ad_soyad TEXT NOT NULL)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS musteriler (id SERIAL PRIMARY KEY, musteri_kodu TEXT UNIQUE NOT NULL, musteri_adi TEXT NOT NULL, tabela_adi TEXT NOT NULL)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS palet_tipleri (id SERIAL PRIMARY KEY, stok_kodu TEXT UNIQUE NOT NULL, palet_adi TEXT NOT NULL)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS stoklar (id SERIAL PRIMARY KEY, stok_sahibi_tip TEXT NOT NULL, stok_sahibi_id INTEGER NOT NULL, palet_tipi_id INTEGER NOT NULL, miktar INTEGER DEFAULT 0, FOREIGN KEY (palet_tipi_id) REFERENCES palet_tipleri(id), UNIQUE(stok_sahibi_tip, stok_sahibi_id, palet_tipi_id))''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS hareketler (id SERIAL PRIMARY KEY, tarih TEXT NOT NULL, yapan_kullanici_id INTEGER NOT NULL, hareket_tipi TEXT NOT NULL, gonderen_tip TEXT NOT NULL, gonderen_id INTEGER NOT NULL, alan_tip TEXT NOT NULL, alan_id INTEGER NOT NULL, palet_tipi_id INTEGER NOT NULL, miktar INTEGER NOT NULL, aciklama TEXT, makbuz_no TEXT, FOREIGN KEY (yapan_kullanici_id) REFERENCES kullanicilar(id), FOREIGN KEY (palet_tipi_id) REFERENCES palet_tipleri(id))''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS makbuzlar (id SERIAL PRIMARY KEY, makbuz_no TEXT UNIQUE NOT NULL, tarih TEXT NOT NULL, islem_tipi TEXT NOT NULL, gonderen_tip TEXT NOT NULL, gonderen_id INTEGER NOT NULL, gonderen_adi TEXT NOT NULL, alan_tip TEXT NOT NULL, alan_id INTEGER NOT NULL, alan_adi TEXT NOT NULL, toplam_miktar INTEGER NOT NULL, aciklama TEXT, yapan_kullanici_id INTEGER NOT NULL, yapan_kullanici_adi TEXT NOT NULL, FOREIGN KEY (yapan_kullanici_id) REFERENCES kullanicilar(id))''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS makbuz_detaylari (id SERIAL PRIMARY KEY, makbuz_id INTEGER NOT NULL, palet_tipi_id INTEGER NOT NULL, stok_kodu TEXT NOT NULL, palet_adi TEXT NOT NULL, miktar INTEGER NOT NULL, FOREIGN KEY (makbuz_id) REFERENCES makbuzlar(id), FOREIGN KEY (palet_tipi_id) REFERENCES palet_tipleri(id))''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS ayarlar (key TEXT PRIMARY KEY, value TEXT NOT NULL)''')
    conn.commit()
    for stok_kodu, palet_adi in PALET_TIPLERI:
        cursor.execute("INSERT INTO palet_tipleri (stok_kodu, palet_adi) SELECT %s, %s WHERE NOT EXISTS (SELECT 1 FROM palet_tipleri WHERE stok_kodu = %s)", (stok_kodu, palet_adi, stok_kodu))
    cursor.execute("INSERT INTO kullanicilar (kullanici_adi, sifre, tip, ad_soyad) SELECT %s, %s, %s, %s WHERE NOT EXISTS (SELECT 1 FROM kullanicilar WHERE kullanici_adi = %s)", ('depocu', hash_sifre('1234'), 'DEPOCU', 'Ana Depocu', 'depocu'))
    conn.commit()
    cursor.execute('SELECT id FROM palet_tipleri')
    for palet in cursor.fetchall():
        cursor.execute("INSERT INTO stoklar (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, miktar) SELECT %s, %s, %s, 0 WHERE NOT EXISTS (SELECT 1 FROM stoklar WHERE stok_sahibi_tip = %s AND stok_sahibi_id = %s AND palet_tipi_id = %s)", ('DEPO', 0, palet[0], 'DEPO', 0, palet[0]))
    conn.commit()
    cursor.close()
    conn.close()


def stok_miktari_getir(stok_sahibi_tip, stok_sahibi_id, palet_tipi_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT miktar FROM stoklar WHERE stok_sahibi_tip = %s AND stok_sahibi_id = %s AND palet_tipi_id = %s", (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id))
    sonuc = cursor.fetchone()
    cursor.close()
    conn.close()
    return sonuc[0] if sonuc else 0


def stok_guncelle(stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, degisim):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, miktar FROM stoklar WHERE stok_sahibi_tip = %s AND stok_sahibi_id = %s AND palet_tipi_id = %s", (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id))
    mevcut = cursor.fetchone()
    if mevcut:
        yeni_miktar = mevcut[1] + degisim
        if yeni_miktar < 0:
            cursor.close()
            conn.close()
            return False, "Stok yetersiz!"
        cursor.execute("UPDATE stoklar SET miktar = %s WHERE id = %s", (yeni_miktar, mevcut[0]))
    else:
        if degisim < 0:
            cursor.close()
            conn.close()
            return False, "Stok kaydı bulunamadı!"
        cursor.execute("INSERT INTO stoklar (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, miktar) VALUES (%s, %s, %s, %s)", (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, degisim))
    conn.commit()
    cursor.close()
    conn.close()
    return True, ""


def hareket_kaydet(yapan_kullanici_id, hareket_tipi, gonderen_tip, gonderen_id, alan_tip, alan_id, palet_tipi_id, miktar, aciklama="", makbuz_no=None):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO hareketler (tarih, yapan_kullanici_id, hareket_tipi, gonderen_tip, gonderen_id, alan_tip, alan_id, palet_tipi_id, miktar, aciklama, makbuz_no) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                   (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), yapan_kullanici_id, hareket_tipi, gonderen_tip, gonderen_id, alan_tip, alan_id, palet_tipi_id, miktar, aciklama, makbuz_no))
    conn.commit()
    cursor.close()
    conn.close()


def makbuz_no_olustur():
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT MAX(CAST(SUBSTRING(makbuz_no, 4) AS INTEGER)) FROM makbuzlar")
        son_makbuz = cursor.fetchone()[0]
    except:
        son_makbuz = None
    yeni_no = (son_makbuz + 1) if son_makbuz else 1
    cursor.close()
    conn.close()
    return f"PLT{str(yeni_no).zfill(5)}"


def makbuz_kaydet(transfer_data):
    conn = get_db_connection()
    cursor = conn.cursor()
    makbuz_no = makbuz_no_olustur()
    tarih = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    cursor.execute("INSERT INTO makbuzlar (makbuz_no, tarih, islem_tipi, gonderen_tip, gonderen_id, gonderen_adi, alan_tip, alan_id, alan_adi, toplam_miktar, aciklama, yapan_kullanici_id, yapan_kullanici_adi) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id",
                   (makbuz_no, tarih, transfer_data['islem_tipi'], transfer_data['gonderen_tip'], transfer_data['gonderen_id'], transfer_data['gonderen_adi'], transfer_data['alan_tip'], transfer_data['alan_id'], transfer_data['alan_adi'], transfer_data['toplam_miktar'], transfer_data['aciklama'], transfer_data['yapan_id'], transfer_data['yapan_adi']))
    makbuz_id = cursor.fetchone()[0]
    for detay in transfer_data['detaylar']:
        cursor.execute("INSERT INTO makbuz_detaylari (makbuz_id, palet_tipi_id, stok_kodu, palet_adi, miktar) VALUES (%s, %s, %s, %s, %s)", (makbuz_id, detay['palet_tipi_id'], detay['stok_kodu'], detay['palet_adi'], detay['miktar']))
    conn.commit()
    cursor.close()
    conn.close()
    return makbuz_no


def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        if not token:
            return jsonify({'hata': 'Token gerekli'}), 401
        try:
            token = token.replace('Bearer ', '')
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
            current_user = data
        except:
            return jsonify({'hata': 'Geçersiz token'}), 401
        return f(current_user, *args, **kwargs)
    return decorated


@app.route('/')
def index():
    return send_from_directory('static', 'index.html')


@app.route('/api/login', methods=['POST'])
def login():
    data = request.get_json()
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, kullanici_adi, tip, ad_soyad FROM kullanicilar WHERE kullanici_adi = %s AND sifre = %s", (data.get('kullanici_adi'), hash_sifre(data.get('sifre'))))
    kullanici = cursor.fetchone()
    cursor.close()
    conn.close()
    if kullanici:
        token = jwt.encode({'id': kullanici[0], 'kullanici_adi': kullanici[1], 'tip': kullanici[2], 'ad_soyad': kullanici[3], 'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=24)}, app.config['SECRET_KEY'], algorithm='HS256')
        return jsonify({'success': True, 'token': token, 'kullanici': {'id': kullanici[0], 'kullanici_adi': kullanici[1], 'tip': kullanici[2], 'ad_soyad': kullanici[3]}})
    return jsonify({'success': False, 'hata': 'Hatalı kullanıcı adı veya şifre'}), 401


@app.route('/api/kullanici_listesi', methods=['GET'])
@token_required
def get_kullanici_listesi(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, kullanici_adi, ad_soyad FROM kullanicilar WHERE tip = 'DAGITICI' ORDER BY ad_soyad")
    sonuc = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify([{'id': k[0], 'kullanici_adi': k[1], 'ad_soyad': k[2]} for k in sonuc])


@app.route('/api/dagitici_listesi', methods=['GET'])
@token_required
def get_dagitici_listesi(current_user):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, kullanici_adi, ad_soyad FROM kullanicilar WHERE tip = 'DAGITICI' ORDER BY ad_soyad")
    sonuc = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify([{'id': d[0], 'kullanici_adi': d[1], 'ad_soyad': d[2]} for d in sonuc])


@app.route('/api/dagitici_ekle', methods=['POST'])
@token_required
def dagitici_ekle(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    data = request.get_json()
    kullanici_adi, ad_soyad, sifre = data.get('kullanici_adi'), data.get('ad_soyad'), data.get('sifre')
    if not kullanici_adi or not ad_soyad or not sifre:
        return jsonify({'hata': 'Tüm alanlar gerekli'}), 400
    if len(sifre) < 4:
        return jsonify({'hata': 'Şifre en az 4 karakter olmalı'}), 400
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO kullanicilar (kullanici_adi, sifre, tip, ad_soyad) VALUES (%s, %s, %s, %s) RETURNING id", (kullanici_adi, hash_sifre(sifre), 'DAGITICI', ad_soyad))
        dagitici_id = cursor.fetchone()[0]
        cursor.execute("SELECT id FROM palet_tipleri")
        for palet in cursor.fetchall():
            cursor.execute("INSERT INTO stoklar (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, miktar) SELECT %s, %s, %s, 0 WHERE NOT EXISTS (SELECT 1 FROM stoklar WHERE stok_sahibi_tip = %s AND stok_sahibi_id = %s AND palet_tipi_id = %s)", ('DAGITICI', dagitici_id, palet[0], 'DAGITICI', dagitici_id, palet[0]))
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'id': dagitici_id, 'mesaj': 'Dağıtıcı eklendi'})
    except Exception as e:
        cursor.close()
        conn.close()
        return jsonify({'hata': str(e)}), 400


@app.route('/api/tum_musteriler', methods=['GET'])
@token_required
def get_tum_musteriler(current_user):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, musteri_kodu, musteri_adi, tabela_adi FROM musteriler ORDER BY musteri_adi")
    sonuc = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify([{'id': m[0], 'musteri_kodu': m[1], 'musteri_adi': m[2], 'tabela_adi': m[3]} for m in sonuc])


@app.route('/api/musteri_ekle', methods=['POST'])
@token_required
def musteri_ekle(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    data = request.get_json()
    musteri_kodu, musteri_adi, tabela_adi = data.get('musteri_kodu'), data.get('musteri_adi'), data.get('tabela_adi')
    if not musteri_kodu or not musteri_adi or not tabela_adi:
        return jsonify({'hata': 'Tüm alanlar gerekli'}), 400
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO musteriler (musteri_kodu, musteri_adi, tabela_adi) VALUES (%s, %s, %s) RETURNING id", (musteri_kodu, musteri_adi, tabela_adi))
        musteri_id = cursor.fetchone()[0]
        cursor.execute("SELECT id FROM palet_tipleri")
        for palet in cursor.fetchall():
            cursor.execute("INSERT INTO stoklar (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, miktar) SELECT %s, %s, %s, 0 WHERE NOT EXISTS (SELECT 1 FROM stoklar WHERE stok_sahibi_tip = %s AND stok_sahibi_id = %s AND palet_tipi_id = %s)", ('MUSTERI', musteri_id, palet[0], 'MUSTERI', musteri_id, palet[0]))
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({'success': True, 'id': musteri_id, 'mesaj': 'Müşteri eklendi'})
    except Exception as e:
        cursor.close()
        conn.close()
        return jsonify({'hata': str(e)}), 400


@app.route('/api/palet_tipleri', methods=['GET'])
@token_required
def get_palet_tipleri(current_user):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, stok_kodu, palet_adi FROM palet_tipleri")
    sonuc = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify([{'id': p[0], 'stok_kodu': p[1], 'palet_adi': p[2]} for p in sonuc])


@app.route('/api/stok', methods=['GET'])
@token_required
def get_stok(current_user):
    tip, kimlik = request.args.get('tip'), request.args.get('id', type=int)
    if not tip or kimlik is None:
        return jsonify({'hata': 'tip ve id parametreleri gerekli'}), 400
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT pt.id, pt.stok_kodu, pt.palet_adi, COALESCE(s.miktar, 0) FROM palet_tipleri pt LEFT JOIN stoklar s ON pt.id = s.palet_tipi_id AND s.stok_sahibi_tip = %s AND s.stok_sahibi_id = %s ORDER BY pt.id", (tip, kimlik))
    sonuc = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify([{'palet_id': p[0], 'stok_kodu': p[1], 'palet_adi': p[2], 'miktar': p[3]} for p in sonuc])


@app.route('/api/transfer', methods=['POST'])
@token_required
def transfer_yap(current_user):
    data = request.get_json()
    hareket_tipi, palet_tipi_id, miktar, alici_id = data.get('hareket_tipi'), data.get('palet_tipi_id'), data.get('miktar'), data.get('alici_id')
    if not hareket_tipi or not palet_tipi_id or not miktar:
        return jsonify({'hata': 'Eksik parametreler'}), 400
    if miktar <= 0:
        return jsonify({'hata': 'Miktar pozitif olmalı'}), 400
    kullanici_id, kullanici_tip, kullanici_adi, kullanici_kadi = current_user['id'], current_user['tip'], current_user['ad_soyad'], current_user['kullanici_adi']
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, stok_kodu, palet_adi FROM palet_tipleri WHERE id = %s", (palet_tipi_id,))
    palet = cursor.fetchone()
    if not palet:
        cursor.close()
        conn.close()
        return jsonify({'hata': 'Geçersiz palet tipi'}), 400
    transfer_data = {'islem_tipi': hareket_tipi, 'yapan_id': kullanici_id, 'yapan_adi': f"{kullanici_adi} ({kullanici_kadi})", 'detaylar': [], 'toplam_miktar': miktar}
    if kullanici_tip == 'DEPOCU':
        if hareket_tipi == 'DEPO_DAGITICI':
            gonderen_tip, gonderen_id, gonderen_adi = SAHIP_TIP_DEPO, 0, "DEPO"
            alan_tip, alan_id = SAHIP_TIP_DAGITICI, alici_id
            cursor.execute("SELECT ad_soyad, kullanici_adi FROM kullanicilar WHERE id = %s AND tip = 'DAGITICI'", (alici_id,))
            dagitici = cursor.fetchone()
            if not dagitici:
                cursor.close()
                conn.close()
                return jsonify({'hata': 'Geçersiz dağıtıcı ID'}), 400
            alan_adi = f"{dagitici[0]} ({dagitici[1]})"
            aciklama = f"{palet[2]} - {miktar} adet {dagitici[0]} dağıtıcısına transfer edildi"
        elif hareket_tipi == 'DAGITICI_DEPO':
            gonderen_tip, gonderen_id = SAHIP_TIP_DAGITICI, alici_id
            alan_tip, alan_id, alan_adi = SAHIP_TIP_DEPO, 0, "DEPO"
            cursor.execute("SELECT ad_soyad, kullanici_adi FROM kullanicilar WHERE id = %s AND tip = 'DAGITICI'", (alici_id,))
            dagitici = cursor.fetchone()
            if not dagitici:
                cursor.close()
                conn.close()
                return jsonify({'hata': 'Geçersiz dağıtıcı ID'}), 400
            gonderen_adi = f"{dagitici[0]} ({dagitici[1]})"
            aciklama = f"{palet[2]} - {miktar} adet {dagitici[0]} dağıtıcısından iade alındı"
        else:
            cursor.close()
            conn.close()
            return jsonify({'hata': 'Geçersiz hareket tipi'}), 400
    elif kullanici_tip == 'DAGITICI':
        if hareket_tipi == 'DAGITICI_MUSTERI':
            gonderen_tip, gonderen_id, gonderen_adi = SAHIP_TIP_DAGITICI, kullanici_id, f"{kullanici_adi} ({kullanici_kadi})"
            alan_tip, alan_id = SAHIP_TIP_MUSTERI, alici_id
            cursor.execute("SELECT musteri_kodu, musteri_adi FROM musteriler WHERE id = %s", (alici_id,))
            musteri = cursor.fetchone()
            if not musteri:
                cursor.close()
                conn.close()
                return jsonify({'hata': 'Geçersiz müşteri ID'}), 400
            alan_adi = f"{musteri[0]} - {musteri[1]}"
            aciklama = f"{palet[2]} - {miktar} adet {musteri[1]} müşterisine verildi"
        elif hareket_tipi == 'MUSTERI_DAGITICI':
            gonderen_tip, gonderen_id = SAHIP_TIP_MUSTERI, alici_id
            alan_tip, alan_id, alan_adi = SAHIP_TIP_DAGITICI, kullanici_id, f"{kullanici_adi} ({kullanici_kadi})"
            cursor.execute("SELECT musteri_kodu, musteri_adi FROM musteriler WHERE id = %s", (alici_id,))
            musteri = cursor.fetchone()
            if not musteri:
                cursor.close()
                conn.close()
                return jsonify({'hata': 'Geçersiz müşteri ID'}), 400
            gonderen_adi = f"{musteri[0]} - {musteri[1]}"
            aciklama = f"{palet[2]} - {miktar} adet {musteri[1]} müşterisinden iade alındı"
        else:
            cursor.close()
            conn.close()
            return jsonify({'hata': 'Geçersiz hareket tipi'}), 400
    else:
        cursor.close()
        conn.close()
        return jsonify({'hata': 'Yetkisiz kullanıcı'}), 403
    transfer_data.update({'gonderen_tip': gonderen_tip, 'gonderen_id': gonderen_id, 'gonderen_adi': gonderen_adi, 'alan_tip': alan_tip, 'alan_id': alan_id, 'alan_adi': alan_adi, 'aciklama': aciklama})
    transfer_data['detaylar'].append({'palet_tipi_id': palet_tipi_id, 'stok_kodu': palet[1], 'palet_adi': palet[2], 'miktar': miktar})
    mevcut_gonderen = stok_miktari_getir(gonderen_tip, gonderen_id, palet_tipi_id)
    if mevcut_gonderen < miktar:
        cursor.close()
        conn.close()
        return jsonify({'hata': f'Yetersiz stok! Mevcut: {mevcut_gonderen}'}), 400
    basarili, hata = stok_guncelle(gonderen_tip, gonderen_id, palet_tipi_id, -miktar)
    if not basarili:
        cursor.close()
        conn.close()
        return jsonify({'hata': hata}), 400
    basarili, hata = stok_guncelle(alan_tip, alan_id, palet_tipi_id, +miktar)
    if not basarili:
        stok_guncelle(gonderen_tip, gonderen_id, palet_tipi_id, +miktar)
        cursor.close()
        conn.close()
        return jsonify({'hata': hata}), 400
    makbuz_no = makbuz_kaydet(transfer_data)
    hareket_kaydet(kullanici_id, hareket_tipi, gonderen_tip, gonderen_id, alan_tip, alan_id, palet_tipi_id, miktar, aciklama, makbuz_no)
    cursor.close()
    conn.close()
    return jsonify({'success': True, 'mesaj': f'Transfer başarılı! {miktar} adet {palet[2]} transfer edildi.', 'makbuz_no': makbuz_no})


@app.route('/api/makbuz/<makbuz_no>', methods=['GET'])
@token_required
def makbuz_goster(makbuz_no):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM makbuzlar WHERE makbuz_no = %s", (makbuz_no,))
    makbuz = cursor.fetchone()
    if not makbuz:
        cursor.close()
        conn.close()
        return jsonify({'hata': 'Makbuz bulunamadı'}), 404
    cursor.execute("SELECT stok_kodu, palet_adi, miktar FROM makbuz_detaylari WHERE makbuz_id = %s", (makbuz[0],))
    detaylar = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify({'makbuz_no': makbuz[1], 'tarih': makbuz[2], 'islem_tipi': makbuz[3], 'gonderen_adi': makbuz[6], 'alan_adi': makbuz[9], 'toplam_miktar': makbuz[10], 'aciklama': makbuz[11], 'yapan_adi': makbuz[13], 'detaylar': [{'stok_kodu': d[0], 'palet_adi': d[1], 'miktar': d[2]} for d in detaylar]})


@app.route('/api/makbuz/pdf/<makbuz_no>', methods=['GET'])
@token_required
def makbuz_pdf(makbuz_no):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM makbuzlar WHERE makbuz_no = %s", (makbuz_no,))
    makbuz = cursor.fetchone()
    if not makbuz:
        cursor.close()
        conn.close()
        return jsonify({'hata': 'Makbuz bulunamadı'}), 404
    cursor.execute("SELECT stok_kodu, palet_adi, miktar FROM makbuz_detaylari WHERE makbuz_id = %s", (makbuz[0],))
    detaylar = cursor.fetchall()
    cursor.close()
    conn.close()
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    styles = getSampleStyleSheet()
    story = []
    title_style = ParagraphStyle('CustomTitle', parent=styles['Heading1'], fontSize=16, alignment=1, spaceAfter=20)
    story.append(Paragraph("ULUDAĞ İÇECEK", title_style))
    story.append(Paragraph("KONYA BÖLGE DEPO", title_style))
    story.append(Paragraph("PALET İŞLEM MAKBUZU", title_style))
    story.append(Spacer(1, 20))
    tip_text = {'DEPO_DAGITICI': 'Depo → Dağıtıcı', 'DAGITICI_MUSTERI': 'Dağıtıcı → Müşteri', 'MUSTERI_DAGITICI': 'Müşteri → Dağıtıcı', 'DAGITICI_DEPO': 'Dağıtıcı → Depo'}.get(makbuz[3], makbuz[3])
    data = [['Makbuz No:', makbuz[1]], ['Tarih:', makbuz[2]], ['İşlem Türü:', tip_text], ['Teslim Eden:', makbuz[6]], ['Teslim Alan:', makbuz[9]], ['Toplam Miktar:', str(makbuz[10])], ['Açıklama:', makbuz[11]], ['İşlemi Yapan:', makbuz[13]]]
    table = Table(data, colWidths=[100, 300])
    table.setStyle(TableStyle([('GRID', (0, 0), (-1, -1), 0.5, colors.grey)]))
    story.append(table)
    story.append(Spacer(1, 20))
    detay_data = [['Stok Kodu', 'Palet Adı', 'Miktar']] + [[d[0], d[1], str(d[2])] for d in detaylar]
    detay_table = Table(detay_data, colWidths=[100, 200, 80])
    detay_table.setStyle(TableStyle([('GRID', (0, 0), (-1, -1), 0.5, colors.grey), ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2196F3')), ('TEXTCOLOR', (0, 0), (-1, 0), colors.white)]))
    story.append(detay_table)
    story.append(Spacer(1, 30))
    story.append(Paragraph("_________________________", styles['Normal']))
    story.append(Paragraph("Yetkili İmza", styles['Normal']))
    doc.build(story)
    buffer.seek(0)
    return send_file(buffer, mimetype='application/pdf', as_attachment=True, download_name=f'makbuz_{makbuz_no}.pdf')


@app.route('/api/hareketler', methods=['GET'])
@token_required
def get_hareketler(current_user):
    limit = request.args.get('limit', 50, type=int)
    conn = get_db_connection()
    cursor = conn.cursor()
    if current_user['tip'] == 'DEPOCU':
        cursor.execute("SELECT h.tarih, u.kullanici_adi, u.ad_soyad, h.hareket_tipi, pt.stok_kodu, pt.palet_adi, h.miktar, h.aciklama, h.makbuz_no FROM hareketler h JOIN kullanicilar u ON h.yapan_kullanici_id = u.id JOIN palet_tipleri pt ON h.palet_tipi_id = pt.id ORDER BY h.tarih DESC LIMIT %s", (limit,))
    else:
        cursor.execute("SELECT h.tarih, u.kullanici_adi, u.ad_soyad, h.hareket_tipi, pt.stok_kodu, pt.palet_adi, h.miktar, h.aciklama, h.makbuz_no FROM hareketler h JOIN kullanicilar u ON h.yapan_kullanici_id = u.id JOIN palet_tipleri pt ON h.palet_tipi_id = pt.id WHERE h.gonderen_id = %s OR h.alan_id = %s ORDER BY h.tarih DESC LIMIT %s", (current_user['id'], current_user['id'], limit))
    sonuc = cursor.fetchall()
    cursor.close()
    conn.close()
    hareketler = []
    for h in sonuc:
        tip_text = {'DEPO_DAGITICI': 'Depo→Dağıtıcı', 'DAGITICI_MUSTERI': 'Dağıtıcı→Müşteri', 'MUSTERI_DAGITICI': 'Müşteri→Dağıtıcı', 'DAGITICI_DEPO': 'Dağıtıcı→Depo', 'DEPO_STOK_HAREKET': 'Depo Stok Hareketi'}.get(h[3], h[3])
        hareketler.append({'tarih': h[0], 'yapan': f"{h[2]} ({h[1]})", 'islem_tipi': tip_text, 'stok_kodu': h[4], 'palet_adi': h[5], 'miktar': h[6], 'aciklama': h[7], 'makbuz_no': h[8]})
    return jsonify(hareketler)


@app.route('/api/hareketler_filtreli', methods=['POST'])
@token_required
def get_hareketler_filtreli(current_user):
    data = request.get_json()
    dagitici_id, palet_tipi_id, baslangic, bitis = data.get('dagitici_id'), data.get('palet_tipi_id'), data.get('baslangic_tarihi'), data.get('bitis_tarihi')
    limit = data.get('limit', 100)
    conn = get_db_connection()
    cursor = conn.cursor()
    query = """SELECT h.tarih, u.kullanici_adi, u.ad_soyad, h.hareket_tipi, pt.stok_kodu, pt.palet_adi, h.miktar, h.aciklama, h.makbuz_no,
               CASE WHEN h.gonderen_tip = 'DAGITICI' THEN (SELECT ad_soyad FROM kullanicilar WHERE id = h.gonderen_id)
                    WHEN h.alan_tip = 'DAGITICI' THEN (SELECT ad_soyad FROM kullanicilar WHERE id = h.alan_id) ELSE NULL END as ilgili_dagitici
               FROM hareketler h JOIN kullanicilar u ON h.yapan_kullanici_id = u.id JOIN palet_tipleri pt ON h.palet_tipi_id = pt.id WHERE 1=1"""
    params = []
    if dagitici_id:
        query += " AND (h.gonderen_id = %s OR h.alan_id = %s)"
        params.extend([dagitici_id, dagitici_id])
    if palet_tipi_id:
        query += " AND h.palet_tipi_id = %s"
        params.append(palet_tipi_id)
    if baslangic and bitis:
        query += " AND DATE(h.tarih) BETWEEN %s AND %s"
        params.extend([baslangic, bitis])
    query += " ORDER BY h.tarih DESC LIMIT %s"
    params.append(limit)
    cursor.execute(query, params)
    sonuc = cursor.fetchall()
    cursor.close()
    conn.close()
    hareketler = []
    for h in sonuc:
        tip_text = {'DEPO_DAGITICI': 'Depo→Dağıtıcı', 'DAGITICI_MUSTERI': 'Dağıtıcı→Müşteri', 'MUSTERI_DAGITICI': 'Müşteri→Dağıtıcı', 'DAGITICI_DEPO': 'Dağıtıcı→Depo', 'DEPO_STOK_HAREKET': 'Depo Stok Hareketi'}.get(h[3], h[3])
        hareketler.append({'tarih': h[0], 'yapan': f"{h[2]} ({h[1]})", 'islem_tipi': tip_text, 'stok_kodu': h[4], 'palet_adi': h[5], 'miktar': h[6], 'aciklama': h[7], 'makbuz_no': h[8], 'ilgili_dagitici': h[9] or '-'})
    return jsonify(hareketler)


@app.route('/api/depo_stok_hareket', methods=['POST'])
@token_required
def depo_stok_hareket(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    data = request.get_json()
    palet_tipi_id, miktar, islem_tipi, aciklama = data.get('palet_tipi_id'), data.get('miktar'), data.get('islem_tipi'), data.get('aciklama', '')
    if not palet_tipi_id or not miktar or not islem_tipi:
        return jsonify({'hata': 'Eksik parametreler'}), 400
    if miktar <= 0:
        return jsonify({'hata': 'Miktar pozitif olmalı'}), 400
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT stok_kodu, palet_adi FROM palet_tipleri WHERE id = %s", (palet_tipi_id,))
    palet = cursor.fetchone()
    if not palet:
        cursor.close()
        conn.close()
        return jsonify({'hata': 'Geçersiz palet tipi'}), 400
    mevcut = stok_miktari_getir(SAHIP_TIP_DEPO, 0, palet_tipi_id)
    if islem_tipi == 'azalt':
        if mevcut < miktar:
            cursor.close()
            conn.close()
            return jsonify({'hata': f'Yetersiz stok! Mevcut: {mevcut}'}), 400
        yeni_miktar, degisim = mevcut - miktar, -miktar
        hareket_aciklama = f"STOK AZALTMA: {miktar} adet {palet[1]} azaltıldı. Sebep: {aciklama}"
    else:
        yeni_miktar, degisim = mevcut + miktar, +miktar
        hareket_aciklama = f"STOK ARTTIRMA: {miktar} adet {palet[1]} eklendi. Sebep: {aciklama}"
    basarili, hata = stok_guncelle(SAHIP_TIP_DEPO, 0, palet_tipi_id, degisim)
    if not basarili:
        cursor.close()
        conn.close()
        return jsonify({'hata': hata}), 400
    hareket_kaydet(current_user['id'], "DEPO_STOK_HAREKET", SAHIP_TIP_DEPO, 0, SAHIP_TIP_DEPO, 0, palet_tipi_id, miktar, hareket_aciklama)
    cursor.close()
    conn.close()
    return jsonify({'success': True, 'mesaj': f'{palet[1]} stoğu güncellendi. Yeni miktar: {yeni_miktar}', 'yeni_miktar': yeni_miktar})


@app.route('/api/rapor/istatistikler', methods=['GET'])
@token_required
def rapor_istatistikler(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT u.kullanici_adi, u.ad_soyad, COUNT(*) FROM hareketler h JOIN kullanicilar u ON h.yapan_kullanici_id = u.id WHERE u.tip = 'DAGITICI' GROUP BY u.id ORDER BY COUNT(*) DESC LIMIT 10")
    en_cok_transfer = [{'kullanici_adi': r[0], 'ad_soyad': r[1], 'transfer_sayisi': r[2]} for r in cursor.fetchall()]
    cursor.execute("SELECT pt.stok_kodu, pt.palet_adi, COUNT(*), SUM(h.miktar) FROM hareketler h JOIN palet_tipleri pt ON h.palet_tipi_id = pt.id WHERE h.hareket_tipi != 'DEPO_STOK_HAREKET' GROUP BY pt.id ORDER BY COUNT(*) DESC")
    en_cok_palet = [{'stok_kodu': r[0], 'palet_adi': r[1], 'kullanim_sayisi': r[2], 'toplam_miktar': r[3]} for r in cursor.fetchall()]
    cursor.close()
    conn.close()
    return jsonify({'en_cok_transfer_yapan': en_cok_transfer, 'en_cok_kullanilan_palet': en_cok_palet})


@app.route('/api/rapor/export', methods=['POST'])
@token_required
def rapor_export(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    data = request.get_json()
    rapor_tipi, baslangic, bitis = data.get('rapor_tipi'), data.get('baslangic_tarihi'), data.get('bitis_tarihi')
    conn = get_db_connection()
    cursor = conn.cursor()
    workbook = openpyxl.Workbook()
    if rapor_tipi == 'hareketler':
        sheet = workbook.active
        sheet.title = "Hareketler"
        headers = ['Tarih', 'Yapan Kullanıcı', 'İşlem Tipi', 'Stok Kodu', 'Palet Adı', 'Miktar', 'Makbuz No', 'Açıklama']
        for col, header in enumerate(headers, 1):
            cell = sheet.cell(row=1, column=col, value=header)
            cell.font = Font(bold=True, color="FFFFFF")
            cell.fill = PatternFill(start_color="2196F3", end_color="2196F3", fill_type="solid")
        if baslangic and bitis:
            cursor.execute("SELECT h.tarih, u.ad_soyad, h.hareket_tipi, pt.stok_kodu, pt.palet_adi, h.miktar, h.makbuz_no, h.aciklama FROM hareketler h JOIN kullanicilar u ON h.yapan_kullanici_id = u.id JOIN palet_tipleri pt ON h.palet_tipi_id = pt.id WHERE DATE(h.tarih) BETWEEN %s AND %s ORDER BY h.tarih DESC", (baslangic, bitis))
        else:
            cursor.execute("SELECT h.tarih, u.ad_soyad, h.hareket_tipi, pt.stok_kodu, pt.palet_adi, h.miktar, h.makbuz_no, h.aciklama FROM hareketler h JOIN kullanicilar u ON h.yapan_kullanici_id = u.id JOIN palet_tipleri pt ON h.palet_tipi_id = pt.id ORDER BY h.tarih DESC LIMIT 1000")
        for row_idx, row in enumerate(cursor.fetchall(), 2):
            tip_text = {'DEPO_DAGITICI': 'Depo→Dağıtıcı', 'DAGITICI_MUSTERI': 'Dağıtıcı→Müşteri', 'MUSTERI_DAGITICI': 'Müşteri→Dağıtıcı', 'DAGITICI_DEPO': 'Dağıtıcı→Depo', 'DEPO_STOK_HAREKET': 'Depo Stok Hareketi'}.get(row[2], row[2])
            sheet.cell(row=row_idx, column=1, value=row[0])
            sheet.cell(row=row_idx, column=2, value=row[1])
            sheet.cell(row=row_idx, column=3, value=tip_text)
            sheet.cell(row=row_idx, column=4, value=row[3])
            sheet.cell(row=row_idx, column=5, value=row[4])
            sheet.cell(row=row_idx, column=6, value=row[5])
            sheet.cell(row=row_idx, column=7, value=row[6] or '-')
            sheet.cell(row=row_idx, column=8, value=row[7] or '')
        for col in range(1, 9):
            sheet.column_dimensions[openpyxl.utils.get_column_letter(col)].width = 18
    elif rapor_tipi == 'stoklar':
        sheet = workbook.active
        sheet.title = "Stoklar"
        headers = ['Stok Sahibi', 'Stok Kodu', 'Palet Adı', 'Miktar']
        for col, header in enumerate(headers, 1):
            cell = sheet.cell(row=1, column=col, value=header)
            cell.font = Font(bold=True, color="FFFFFF")
            cell.fill = PatternFill(start_color="2196F3", end_color="2196F3", fill_type="solid")
        row_idx = 2
        cursor.execute("SELECT pt.stok_kodu, pt.palet_adi, s.miktar FROM stoklar s JOIN palet_tipleri pt ON s.palet_tipi_id = pt.id WHERE s.stok_sahibi_tip = 'DEPO'")
        for row in cursor.fetchall():
            sheet.cell(row=row_idx, column=1, value="DEPO")
            sheet.cell(row=row_idx, column=2, value=row[0])
            sheet.cell(row=row_idx, column=3, value=row[1])
            sheet.cell(row=row_idx, column=4, value=row[2])
            row_idx += 1
        cursor.execute("SELECT u.ad_soyad, pt.stok_kodu, pt.palet_adi, s.miktar FROM stoklar s JOIN kullanicilar u ON s.stok_sahibi_id = u.id JOIN palet_tipleri pt ON s.palet_tipi_id = pt.id WHERE s.stok_sahibi_tip = 'DAGITICI' AND s.miktar > 0 ORDER BY u.ad_soyad")
        for row in cursor.fetchall():
            sheet.cell(row=row_idx, column=1, value=row[0])
            sheet.cell(row=row_idx, column=2, value=row[1])
            sheet.cell(row=row_idx, column=3, value=row[2])
            sheet.cell(row=row_idx, column=4, value=row[3])
            row_idx += 1
        cursor.execute("SELECT m.musteri_kodu, m.musteri_adi, pt.stok_kodu, pt.palet_adi, s.miktar FROM stoklar s JOIN musteriler m ON s.stok_sahibi_id = m.id JOIN palet_tipleri pt ON s.palet_tipi_id = pt.id WHERE s.stok_sahibi_tip = 'MUSTERI' AND s.miktar > 0 ORDER BY m.musteri_adi")
        for row in cursor.fetchall():
            sheet.cell(row=row_idx, column=1, value=f"{row[0]} - {row[1]}")
            sheet.cell(row=row_idx, column=2, value=row[2])
            sheet.cell(row=row_idx, column=3, value=row[3])
            sheet.cell(row=row_idx, column=4, value=row[4])
            row_idx += 1
        for col in range(1, 5):
            sheet.column_dimensions[openpyxl.utils.get_column_letter(col)].width = 25
    elif rapor_tipi == 'musteriler':
        sheet = workbook.active
        sheet.title = "Müşteriler"
        headers = ['Müşteri Kodu', 'Müşteri Adı', 'Tabela Adı']
        for col, header in enumerate(headers, 1):
            cell = sheet.cell(row=1, column=col, value=header)
            cell.font = Font(bold=True, color="FFFFFF")
            cell.fill = PatternFill(start_color="2196F3", end_color="2196F3", fill_type="solid")
        cursor.execute("SELECT musteri_kodu, musteri_adi, tabela_adi FROM musteriler ORDER BY musteri_adi")
        for row_idx, row in enumerate(cursor.fetchall(), 2):
            sheet.cell(row=row_idx, column=1, value=row[0])
            sheet.cell(row=row_idx, column=2, value=row[1])
            sheet.cell(row=row_idx, column=3, value=row[2])
        for col in range(1, 4):
            sheet.column_dimensions[openpyxl.utils.get_column_letter(col)].width = 25
    cursor.close()
    conn.close()
    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name=f'{rapor_tipi}_raporu.xlsx')


@app.route('/api/rapor/pdf', methods=['POST'])
@token_required
def rapor_pdf(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    data = request.get_json()
    rapor_tipi = data.get('rapor_tipi', 'hareketler')
    baslangic = data.get('baslangic_tarihi')
    bitis = data.get('bitis_tarihi')
    conn = get_db_connection()
    cursor = conn.cursor()
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(A4))
    styles = getSampleStyleSheet()
    story = []
    title_style = ParagraphStyle('CustomTitle', parent=styles['Heading1'], fontSize=16, alignment=1, spaceAfter=20)
    story.append(Paragraph("ULUDAĞ İÇECEK KONYA BÖLGE DEPO", title_style))
    story.append(Paragraph(f"{rapor_tipi.upper()} RAPORU", title_style))
    story.append(Spacer(1, 10))
    if baslangic and bitis:
        date_text = f"Tarih Aralığı: {baslangic} - {bitis}"
    else:
        date_text = f"Oluşturma Tarihi: {datetime.datetime.now().strftime('%d.%m.%Y %H:%M')}"
    story.append(Paragraph(date_text, styles['Normal']))
    story.append(Spacer(1, 20))
    if rapor_tipi == 'hareketler':
        if baslangic and bitis:
            cursor.execute('''SELECT h.tarih, u.ad_soyad, h.hareket_tipi, pt.stok_kodu, pt.palet_adi, h.miktar, h.aciklama, h.makbuz_no FROM hareketler h JOIN kullanicilar u ON h.yapan_kullanici_id = u.id JOIN palet_tipleri pt ON h.palet_tipi_id = pt.id WHERE DATE(h.tarih) BETWEEN %s AND %s ORDER BY h.tarih DESC''', (baslangic, bitis))
        else:
            cursor.execute('''SELECT h.tarih, u.ad_soyad, h.hareket_tipi, pt.stok_kodu, pt.palet_adi, h.miktar, h.aciklama, h.makbuz_no FROM hareketler h JOIN kullanicilar u ON h.yapan_kullanici_id = u.id JOIN palet_tipleri pt ON h.palet_tipi_id = pt.id ORDER BY h.tarih DESC LIMIT 500''')
        sonuc = cursor.fetchall()
        data = [['Tarih', 'Yapan', 'İşlem Tipi', 'Stok Kodu', 'Palet Adı', 'Miktar', 'Makbuz No', 'Açıklama']]
        for row in sonuc:
            tip_text = {'DEPO_DAGITICI': 'Depo→Dağıtıcı', 'DAGITICI_MUSTERI': 'Dağıtıcı→Müşteri', 'MUSTERI_DAGITICI': 'Müşteri→Dağıtıcı', 'DAGITICI_DEPO': 'Dağıtıcı→Depo', 'DEPO_STOK_HAREKET': 'Depo Stok Hareketi'}.get(row[2], row[2])
            data.append([row[0][:16], row[1], tip_text, row[3], row[4], str(row[5]), row[7] or '-', (row[6][:40] + '...') if row[6] and len(row[6]) > 40 else (row[6] or '')])
        table = Table(data, colWidths=[80, 70, 80, 60, 70, 40, 90, 100])
        table.setStyle(TableStyle([('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2196F3')), ('TEXTCOLOR', (0, 0), (-1, 0), colors.white), ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'), ('FONTSIZE', (0, 0), (-1, 0), 8), ('FONTSIZE', (0, 1), (-1, -1), 7), ('GRID', (0, 0), (-1, -1), 0.5, colors.grey)]))
        story.append(table)
    elif rapor_tipi == 'stoklar':
        cursor.execute('SELECT pt.stok_kodu, pt.palet_adi, s.miktar FROM stoklar s JOIN palet_tipleri pt ON s.palet_tipi_id = pt.id WHERE s.stok_sahibi_tip = %s', ('DEPO',))
        depo_stok = cursor.fetchall()
        story.append(Paragraph("📦 DEPO STOĞU", styles['Heading2']))
        data = [['Stok Kodu', 'Palet Adı', 'Miktar']] + [[r[0], r[1], str(r[2])] for r in depo_stok]
        table = Table(data, colWidths=[100, 200, 80])
        table.setStyle(TableStyle([('GRID', (0, 0), (-1, -1), 0.5, colors.grey)]))
        story.append(table)
        story.append(Spacer(1, 20))
        cursor.execute('''SELECT u.ad_soyad, pt.stok_kodu, pt.palet_adi, s.miktar FROM stoklar s JOIN kullanicilar u ON s.stok_sahibi_id = u.id JOIN palet_tipleri pt ON s.palet_tipi_id = pt.id WHERE s.stok_sahibi_tip = 'DAGITICI' AND s.miktar > 0 ORDER BY u.ad_soyad''')
        dagitici_stok = cursor.fetchall()
        story.append(Paragraph("🚚 DAĞITICI STOKLARI", styles['Heading2']))
        data = [['Dağıtıcı', 'Stok Kodu', 'Palet Adı', 'Miktar']] + [[r[0], r[1], r[2], str(r[3])] for r in dagitici_stok]
        table = Table(data, colWidths=[100, 80, 120, 60])
        table.setStyle(TableStyle([('GRID', (0, 0), (-1, -1), 0.5, colors.grey)]))
        story.append(table)
        story.append(Spacer(1, 20))
        cursor.execute('''SELECT m.musteri_kodu, m.musteri_adi, pt.stok_kodu, pt.palet_adi, s.miktar FROM stoklar s JOIN musteriler m ON s.stok_sahibi_id = m.id JOIN palet_tipleri pt ON s.palet_tipi_id = pt.id WHERE s.stok_sahibi_tip = 'MUSTERI' AND s.miktar > 0 ORDER BY m.musteri_adi LIMIT 100''')
        musteri_stok = cursor.fetchall()
        story.append(Paragraph("🏪 MÜŞTERİ STOKLARI", styles['Heading2']))
        data = [['Müşteri', 'Stok Kodu', 'Palet Adı', 'Miktar']] + [[f"{r[0]} - {r[1]}", r[2], r[3], str(r[4])] for r in musteri_stok]
        table = Table(data, colWidths=[150, 80, 100, 60])
        table.setStyle(TableStyle([('GRID', (0, 0), (-1, -1), 0.5, colors.grey)]))
        story.append(table)
    elif rapor_tipi == 'istatistik':
        cursor.execute('''SELECT u.ad_soyad, COUNT(*) as sayi FROM hareketler h JOIN kullanicilar u ON h.yapan_kullanici_id = u.id WHERE u.tip = 'DAGITICI' GROUP BY u.id ORDER BY sayi DESC LIMIT 10''')
        en_cok = cursor.fetchall()
        story.append(Paragraph("🏆 EN ÇOK TRANSFER YAPAN DAĞITICILAR", styles['Heading2']))
        data = [['Dağıtıcı', 'Transfer Sayısı']] + [[r[0], str(r[1])] for r in en_cok]
        table = Table(data, colWidths=[200, 100])
        table.setStyle(TableStyle([('GRID', (0, 0), (-1, -1), 0.5, colors.grey)]))
        story.append(table)
        story.append(Spacer(1, 20))
        cursor.execute('''SELECT pt.palet_adi, COUNT(*) as sayi, SUM(h.miktar) as toplam FROM hareketler h JOIN palet_tipleri pt ON h.palet_tipi_id = pt.id GROUP BY pt.id ORDER BY sayi DESC''')
        palet_kullanim = cursor.fetchall()
        story.append(Paragraph("📦 EN ÇOK KULLANILAN PALET TİPLERİ", styles['Heading2']))
        data = [['Palet Tipi', 'Kullanım Sayısı', 'Toplam Miktar']] + [[r[0], str(r[1]), str(r[2])] for r in palet_kullanim]
        table = Table(data, colWidths=[150, 100, 100])
        table.setStyle(TableStyle([('GRID', (0, 0), (-1, -1), 0.5, colors.grey)]))
        story.append(table)
    cursor.close()
    conn.close()
    doc.build(story)
    buffer.seek(0)
    return send_file(buffer, mimetype='application/pdf', as_attachment=True, download_name=f'{rapor_tipi}_raporu_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.pdf')


@app.route('/api/musteri_excel_yukle', methods=['POST'])
@token_required
def musteri_excel_yukle(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    if 'file' not in request.files:
        return jsonify({'hata': 'Dosya bulunamadı'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'hata': 'Dosya seçilmedi'}), 400
    try:
        workbook = openpyxl.load_workbook(file)
        sheet = workbook.active
    except Exception as e:
        return jsonify({'hata': f'Excel okunamadı: {str(e)}'}), 400
    headers = [str(sheet.cell(row=1, column=col).value or '').strip().upper() for col in range(1, sheet.max_column + 1)]
    required_columns = ['MÜŞTERİ KODU', 'MÜŞTERİ ADI', 'TABELA ADI']
    for col in required_columns:
        if col not in headers:
            return jsonify({'hata': f"'{col}' sütunu bulunamadı"}), 400
    col_indices = {h: i+1 for i, h in enumerate(headers)}
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT musteri_kodu, id FROM musteriler')
    mevcut_musteriler = {m[0]: m[1] for m in cursor.fetchall()}
    cursor.execute('SELECT id FROM palet_tipleri')
    paletler = [p[0] for p in cursor.fetchall()]
    eklenen = guncellenen = 0
    hatalar = []
    for row in range(2, sheet.max_row + 1):
        musteri_kodu = str(sheet.cell(row=row, column=col_indices['MÜŞTERİ KODU']).value or '').strip()
        musteri_adi = str(sheet.cell(row=row, column=col_indices['MÜŞTERİ ADI']).value or '').strip()
        tabela_adi = str(sheet.cell(row=row, column=col_indices['TABELA ADI']).value or '').strip()
        if not musteri_kodu or not musteri_adi or not tabela_adi:
            hatalar.append(f"Satır {row}: Boş alan var")
            continue
        if musteri_kodu in mevcut_musteriler:
            try:
                cursor.execute("UPDATE musteriler SET musteri_adi = %s, tabela_adi = %s WHERE musteri_kodu = %s", (musteri_adi, tabela_adi, musteri_kodu))
                guncellenen += 1
            except Exception as e:
                hatalar.append(f"Satır {row}: Güncelleme hatası - {str(e)}")
        else:
            try:
                cursor.execute("INSERT INTO musteriler (musteri_kodu, musteri_adi, tabela_adi) VALUES (%s, %s, %s) RETURNING id", (musteri_kodu, musteri_adi, tabela_adi))
                musteri_id = cursor.fetchone()[0]
                for palet_id in paletler:
                    cursor.execute("INSERT INTO stoklar (stok_sahibi_tip, stok_sahibi_id, palet_tipi_id, miktar) SELECT %s, %s, %s, 0 WHERE NOT EXISTS (SELECT 1 FROM stoklar WHERE stok_sahibi_tip = %s AND stok_sahibi_id = %s AND palet_tipi_id = %s)", ('MUSTERI', musteri_id, palet_id, 'MUSTERI', musteri_id, palet_id))
                eklenen += 1
                mevcut_musteriler[musteri_kodu] = musteri_id
            except Exception as e:
                hatalar.append(f"Satır {row}: Ekleme hatası - {str(e)}")
    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({'success': True, 'eklenen': eklenen, 'guncellenen': guncellenen, 'hatalar': hatalar, 'mesaj': f'{eklenen} yeni, {guncellenen} güncellendi.'})


@app.route('/api/yedekle', methods=['GET'])
@token_required
def yedekle(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    conn = get_db_connection()
    cursor = conn.cursor()
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for table in ['hareketler', 'stoklar', 'musteriler', 'kullanicilar', 'palet_tipleri', 'makbuzlar', 'makbuz_detaylari']:
            cursor.execute(f"SELECT * FROM {table}")
            rows = cursor.fetchall()
            if rows:
                csv_buffer = BytesIO()
                csv_writer = csv.writer(csv_buffer)
                csv_writer.writerow([desc[0] for desc in cursor.description])
                csv_writer.writerows(rows)
                zipf.writestr(f'{table}.csv', csv_buffer.getvalue().decode('utf-8'))
        info = f"Palet Takip Sistemi Yedeği\nOluşturma: {datetime.datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\nOluşturan: {current_user['ad_soyad']}"
        zipf.writestr('yedek_bilgi.txt', info)
    cursor.close()
    conn.close()
    buffer.seek(0)
    return send_file(buffer, mimetype='application/zip', as_attachment=True, download_name=f'palet_takip_yedek_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.zip')


@app.route('/api/yedekleme_ayarla', methods=['POST'])
@token_required
def yedekleme_ayarla(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    data = request.get_json()
    aktif, periyot, saat = data.get('aktif', False), data.get('periyot', 'gunluk'), data.get('saat', '03:00')
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO ayarlar (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", ('yedekleme_aktif', str(aktif)))
    cursor.execute("INSERT INTO ayarlar (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", ('yedekleme_periyot', periyot))
    cursor.execute("INSERT INTO ayarlar (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", ('yedekleme_saat', saat))
    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({'success': True, 'mesaj': f'Otomatik yedekleme {"açıldı" if aktif else "kapatıldı"}. Periyot: {periyot}, Saat: {saat}'})


@app.route('/api/yedekleme_ayarlari', methods=['GET'])
@token_required
def yedekleme_ayarlari(current_user):
    if current_user['tip'] != 'DEPOCU':
        return jsonify({'hata': 'Yetkisiz erişim'}), 403
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT key, value FROM ayarlar WHERE key LIKE 'yedekleme_%'")
    sonuc = cursor.fetchall()
    cursor.close()
    conn.close()
    ayarlar = {row[0].replace('yedekleme_', ''): row[1] for row in sonuc}
    return jsonify({'aktif': ayarlar.get('aktif', 'False') == 'True', 'periyot': ayarlar.get('periyot', 'gunluk'), 'saat': ayarlar.get('saat', '03:00')})


if __name__ == '__main__':
    veritabani_olustur()
    from waitress import serve
    port = int(os.environ.get('PORT', 5000))
    serve(app, host='0.0.0.0', port=port, threads=4)
